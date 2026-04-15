import asyncio
import json
import asyncpg
from datetime import datetime, timezone
from typing import Any
from temporalio import activity


CRM_CONFIGS = {
    "integritasmrv": {
        "host": "10.0.13.2",
        "port": 5432,
        "db": "integritasmrv_crm",
        "user": "integritasmrv_crm_user",
        "password": "Int3gr1t@smrv_S3cure_P@ssw0rd_2026",
    },
    "poweriq": {
        "host": "10.0.14.2",
        "port": 5432,
        "db": "poweriq_crm",
        "user": "poweriq_crm_user",
        "password": "P0w3r1Q_CRM_S3cur3_P@ss_2026",
    },
}


@activity.defn
async def upsert_crm_entity(
    mapped_data: dict,
    target_crm: str,
    table: str = "nb_crm_contacts",
    business_key_value: str = None,
) -> dict:
    """
    Upsert for NocoBase CRM tables.
    Uses hubspot_id as the unique key for upsert matching.
    Maps flat fields to direct columns, extra fields to JSONB 'extra' column.
    """
    config = CRM_CONFIGS.get(target_crm)
    if not config:
        raise ValueError(f"Unknown CRM: {target_crm}")

    conn = await asyncpg.connect(
        host=config["host"],
        port=config["port"],
        database=config["db"],
        user=config["user"],
        password=config["password"],
    )

    try:
        # Direct columns that map straight to table columns
        direct_columns = [
            "name", "email", "phone", "title", "website", "city", "country",
            "description", "industry", "linkedin", "hubspot_id", "hs_owner_id",
            "customer_id", "account_number", "vat_number", "location_country"
        ]
        
        # Fields that go into the 'extra' JSONB column
        extra_data = {}
        
        # Build the SET clause for UPDATE and column list for INSERT
        set_clauses = []
        col_names = []
        col_values = []
        param_idx = 1
        
        # Map known fields
        known_fields = {
            "name": "name", "email": "email", "phone": "phone", 
            "title": "title", "website": "website", "city": "city",
            "country": "country", "description": "description", 
            "industry": "industry", "linkedin": "linkedin",
            "hubspot_id": "hubspot_id", "hs_owner_id": "hs_owner_id",
            "customer_id": "customer_id", "vat_number": "vat_number",
            "location_country": "location_country"
        }
        
        for source_key, target_col in known_fields.items():
            if source_key in mapped_data and mapped_data[source_key] is not None:
                col_names.append(target_col)
                col_values.append(mapped_data[source_key])
                set_clauses.append(f"{target_col} = ${param_idx}")
                param_idx += 1
        
        # Handle extra fields (anything not a direct column)
        for key, value in mapped_data.items():
            if key not in known_fields and value is not None:
                if key.startswith("extra."):
                    extra_data[key[6:]] = value
                else:
                    extra_data[key] = value
        
        # Add enrichment_status
        enrichment_status = mapped_data.get("enrichment_status", "pending")
        set_clauses.append(f"enrichment_status = ${param_idx}")
        col_names.append("enrichment_status")
        col_values.append(enrichment_status)
        param_idx += 1
        
        # Set extra JSONB
        if extra_data:
            set_clauses.append(f"extra = ${param_idx}")
            col_names.append("extra")
            col_values.append(json.dumps(extra_data))
            param_idx += 1
        
        # Always update timestamp
        set_clauses.append('"updatedAt" = NOW()')
        
        hubspot_id = mapped_data.get("hubspot_id")
        
        if hubspot_id:
            # Check if record exists
            existing = await conn.fetchrow(
                f'SELECT id FROM {table} WHERE hubspot_id = $1',
                str(hubspot_id)
            )
            
            if existing:
                # UPDATE
                update_sql = f"""
                    UPDATE {table} 
                    SET {', '.join(set_clauses)}
                    WHERE hubspot_id = ${param_idx}
                    RETURNING id, FALSE as created
                """
                col_values.append(str(hubspot_id))
                record = await conn.fetchrow(update_sql, *col_values)
                return {"id": record["id"], "created": False}
            else:
                # INSERT
                col_names.append('"createdAt"')
                col_values.append(datetime.now(timezone.utc))
                insert_sql = f"""
                    INSERT INTO {table} ({', '.join(col_names)})
                    VALUES ({', '.join(['$' + str(i) for i in range(1, len(col_values) + 1)])})
                    RETURNING id, TRUE as created
                """
                record = await conn.fetchrow(insert_sql, *col_values)
                return {"id": record["id"], "created": record["created"]}
        else:
            # No hubspot_id, just insert
            col_names.append('"createdAt"')
            col_values.append(datetime.now(timezone.utc))
            insert_sql = f"""
                INSERT INTO {table} ({', '.join(col_names)})
                VALUES ({', '.join(['$' + str(i) for i in range(1, len(col_values) + 1)])})
                RETURNING id, TRUE as created
            """
            record = await conn.fetchrow(insert_sql, *col_values)
            return {"id": record["id"], "created": record["created"]}
    finally:
        await conn.close()


@activity.defn
async def get_crm_entity(
    target_crm: str,
    table: str,
    entity_id: int,
) -> dict:
    config = CRM_CONFIGS.get(target_crm)
    if not config:
        raise ValueError(f"Unknown CRM: {target_crm}")

    conn = await asyncpg.connect(
        host=config["host"],
        port=config["port"],
        database=config["db"],
        user=config["user"],
        password=config["password"],
    )
    try:
        row = await conn.fetchrow(
            f"SELECT * FROM {table} WHERE id = $1", entity_id
        )
        return dict(row) if row else None
    finally:
        await conn.close()


@activity.defn
async def check_entity_exists(
    external_ids: dict = None,
    contact_data: dict = None,
    company_data: dict = None,
    target_crm: str = "integritasmrv",
) -> dict:
    """
    Check if contact AND/OR company already exists in CRM.
    
    Checks both nb_crm_contacts and nb_crm_customers tables.
    Returns exact matches AND fuzzy matches so caller can decide:
      - Contact is new but company exists → link to company
      - Contact exists → update/merge
      - Company exists → update/merge
      - Neither exists → create both
    
    Args:
        external_ids: Dict with any of: hubspot_id, hubspot_company_id, kbo_id, vat_number
        contact_data: Dict with: email, phone, linkedin_url, firstname, lastname
        company_data: Dict with: name, website, linkedin_url, country, city
        target_crm: "integritasmrv" or "poweriq"
    
    Returns:
        {
            "contact": {
                "exact_match": {"exists": bool, "entity_id": int|None, "label": str|None},
                "fuzzy_matches": [{"entity_id": int, "label": str, "score": float, "match_type": str}],
                "action": "create" / "update" / "merge" / "link_to_company"
            },
            "company": {
                "exact_match": {"exists": bool, "entity_id": int|None, "label": str|None},
                "fuzzy_matches": [{"entity_id": int, "label": str, "score": float, "match_type": str}],
                "action": "create" / "update" / "merge"
            }
        }
    """
    import asyncio
    
    external_ids = external_ids or {}
    contact_data = contact_data or {}
    company_data = company_data or {}

    config = CRM_CONFIGS.get(target_crm)
    if not config:
        raise ValueError(f"Unknown CRM: {target_crm}")

    conn = await asyncpg.connect(
        host=config["host"],
        port=config["port"],
        database=config["db"],
        user=config["user"],
        password=config["password"],
    )

    async def check_contact_exact() -> dict:
        lookups = []
        params = []
        if external_ids.get("hubspot_id"):
            params.append(str(external_ids["hubspot_id"]))
            lookups.append(f"external_ids->>'hubspot_id' = ${len(params)}")
        if external_ids.get("kbo_id"):
            params.append(external_ids["kbo_id"])
            lookups.append(f"external_ids->>'kbo_id' = ${len(params)}")
        if external_ids.get("vat_number"):
            params.append(external_ids["vat_number"])
            lookups.append(f"external_ids->>'vat_number' = ${len(params)}")
        if contact_data.get("email"):
            params.append(contact_data["email"].lower().strip())
            lookups.append(f"lower(entity_attributes->>'email') = ${len(params)}")
        if not lookups:
            return {"exists": False, "entity_id": None, "label": None}
        query = f"SELECT id, label, enrichment_status FROM nb_crm_contacts WHERE {' OR '.join(lookups)} LIMIT 1"
        row = await conn.fetchrow(query, *params)
        return {
            "exists": row is not None,
            "entity_id": row["id"] if row else None,
            "label": row["label"] if row else None,
            "enrichment_status": row["enrichment_status"] if row else None,
        }

    async def check_contact_fuzzy() -> list:
        matches = []
        name = contact_data.get("firstname", "") + " " + contact_data.get("lastname", "")
        name = name.strip().lower()
        phone = contact_data.get("phone")
        linkedin = contact_data.get("linkedin_url")

        if not name and not phone and not linkedin:
            return []

        base_query = "SELECT id, label, entity_attributes FROM nb_crm_contacts WHERE "
        conditions = []
        params = []

        if name:
            params.append(name)
            conditions.append(f"lower(label) = ${len(params)}")
        if phone:
            params.append(phone)
            conditions.append(f"entity_attributes->>'phone' = ${len(params)}")

        if not conditions:
            return []

        try:
            query = base_query + " OR ".join(conditions) + " LIMIT 5"
            rows = await conn.fetch(query, *params)
            for row in rows:
                score = 0.0
                match_type = []
                stored_name = (row["label"] or "").lower()
                stored_phone = (row["entity_attributes"] or {}).get("phone", "")
                stored_linkedin = (row["entity_attributes"] or {}).get("linkedin_url", "")

                if name and stored_name == name:
                    score += 0.7
                    match_type.append("name")
                if phone and stored_phone == phone:
                    score += 0.3
                    match_type.append("phone")
                if linkedin and stored_linkedin == linkedin:
                    score += 0.5
                    match_type.append("linkedin")

                if score >= 0.5:
                    matches.append({
                        "entity_id": row["id"],
                        "label": row["label"],
                        "score": min(score, 1.0),
                        "match_type": "+".join(match_type) if match_type else "fuzzy"
                    })
        except Exception:
            pass
        return sorted(matches, key=lambda x: x["score"], reverse=True)[:3]

    async def check_company_exact() -> dict:
        lookups = []
        params = []
        if external_ids.get("hubspot_company_id"):
            params.append(str(external_ids["hubspot_company_id"]))
            lookups.append(f"external_ids->>'hubspot_company_id' = ${len(params)}")
        if external_ids.get("kbo_id"):
            params.append(external_ids["kbo_id"])
            lookups.append(f"external_ids->>'kbo_id' = ${len(params)}")
        if external_ids.get("vat_number"):
            params.append(external_ids["vat_number"])
            lookups.append(f"external_ids->>'vat_number' = ${len(params)}")
        website = company_data.get("website", "").lower().strip()
        if website:
            params.append(website)
            lookups.append(f"lower(entity_attributes->>'website') = ${len(params)}")
        if not lookups:
            return {"exists": False, "entity_id": None, "label": None}
        query = f"SELECT id, label, enrichment_status FROM nb_crm_customers WHERE {' OR '.join(lookups)} LIMIT 1"
        row = await conn.fetchrow(query, *params)
        return {
            "exists": row is not None,
            "entity_id": row["id"] if row else None,
            "label": row["label"] if row else None,
            "enrichment_status": row["enrichment_status"] if row else None,
        }

    async def check_company_fuzzy() -> list:
        matches = []
        name = (company_data.get("name") or "").strip().lower()
        website = (company_data.get("website") or "").lower().strip()
        country = (company_data.get("country") or "").lower().strip()
        city = (company_data.get("city") or "").lower().strip()
        linkedin = company_data.get("linkedin_url")

        if not name and not website:
            return []

        conditions = []
        params = []

        if name:
            params.append(name)
            conditions.append(f"lower(label) = ${len(params)}")
        if website:
            params.append(website)
            conditions.append(f"lower(entity_attributes->>'website') = ${len(params)}")

        if not conditions:
            return []

        try:
            query = f"SELECT id, label, entity_attributes FROM nb_crm_customers WHERE {' OR '.join(conditions)} LIMIT 10"
            rows = await conn.fetch(query, *params)
            for row in rows:
                score = 0.0
                match_type = []
                stored_name = (row["label"] or "").lower()
                stored_website = (row["entity_attributes"] or {}).get("website", "").lower()
                stored_country = (row["entity_attributes"] or {}).get("country", "").lower()
                stored_city = (row["entity_attributes"] or {}).get("city", "").lower()
                stored_linkedin = (row["entity_attributes"] or {}).get("linkedin_url", "")

                if name and stored_name == name:
                    score += 0.5
                    match_type.append("name")
                if website and stored_website == website:
                    score += 0.5
                    match_type.append("website")
                if country and stored_country == country:
                    score += 0.1
                    match_type.append("country")
                if city and stored_city == city:
                    score += 0.1
                    match_type.append("city")
                if linkedin and stored_linkedin == linkedin:
                    score += 0.3
                    match_type.append("linkedin")

                if score >= 0.5:
                    matches.append({
                        "entity_id": row["id"],
                        "label": row["label"],
                        "score": min(score, 1.0),
                        "match_type": "+".join(match_type) if match_type else "fuzzy"
                    })
        except Exception:
            pass
        return sorted(matches, key=lambda x: x["score"], reverse=True)[:3]

    try:
        contact_exact, contact_fuzzy, company_exact, company_fuzzy = await asyncio.gather(
            check_contact_exact(), check_contact_fuzzy(),
            check_company_exact(), check_company_fuzzy()
        )

        contact_action = "create"
        if contact_exact["exists"]:
            contact_action = "update"
        elif contact_fuzzy and contact_fuzzy[0]["score"] >= 0.9:
            contact_action = "merge"
        elif contact_fuzzy and contact_fuzzy[0]["score"] >= 0.5:
            contact_action = "review"

        company_action = "create"
        if company_exact["exists"]:
            company_action = "update"
        elif company_fuzzy and company_fuzzy[0]["score"] >= 0.9:
            company_action = "merge"
        elif company_fuzzy and company_fuzzy[0]["score"] >= 0.5:
            company_action = "review"

        return {
            "contact": {
                "exact_match": contact_exact,
                "fuzzy_matches": contact_fuzzy,
                "action": contact_action,
            },
            "company": {
                "exact_match": company_exact,
                "fuzzy_matches": company_fuzzy,
                "action": company_action,
            },
        }
    finally:
        await conn.close()

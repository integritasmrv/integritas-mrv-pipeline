import asyncpg
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
    business_key_field: str = "external_ids",
    business_key_value: str = None,
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
        entity_attrs = mapped_data.get("entity_attributes", {})
        external_ids = mapped_data.get("external_ids", {})
        enrichment_status = mapped_data.get("enrichment_status", "pending")

        external_ids["business_key"] = business_key_value

        record = await conn.fetchrow(
            """
            INSERT INTO nb_crm_contacts (
                label, source_system, external_ids, entity_attributes,
                enrichment_status, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
            ON CONFLICT (external_ids->>'business_key')
            DO UPDATE SET
                label = EXCLUDED.label,
                source_system = EXCLUDED.source_system,
                external_ids = EXCLUDED.external_ids,
                entity_attributes = EXCLUDED.entity_attributes,
                enrichment_status = EXCLUDED.enrichment_status,
                updated_at = NOW()
            RETURNING id, created_at
            """,
            mapped_data.get("label"),
            mapped_data.get("source_system"),
            external_ids,
            entity_attrs,
            enrichment_status,
        )
        return {"id": record["id"], "created": record["created_at"]}
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
    external_ids: dict,
    target_crm: str = "integritasmrv",
    table: str = "nb_crm_contacts",
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
        if external_ids.get("email"):
            params.append(external_ids["email"])
            lookups.append(f"entity_attributes->>'email' = ${len(params)}")

        if not lookups:
            return {"exists": False, "entity_id": None}

        query = f"SELECT id, label, enrichment_status FROM {table} WHERE {' OR '.join(lookups)} LIMIT 1"
        row = await conn.fetchrow(query, *params)
        return {
            "exists": row is not None,
            "entity_id": row["id"] if row else None,
            "label": row["label"] if row else None,
            "enrichment_status": row["enrichment_status"] if row else None,
        }
    finally:
        await conn.close()

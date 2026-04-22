
async def write_hubspot_trusted(entity_id: str, crm_id: str | None,
                                crm_name: str = "integritasmrv",
                                hubspot_id: str | None = None) -> str:
    if not HS_TOKEN:
        return "skipped_no_token"
    conn = await get_enrichment_conn()
    try:
        rows = await conn.fetch(
            """SELECT attr_key, attr_value, confidence
               FROM entity_attributes
               WHERE entity_id=$1::uuid AND is_trusted=true
               ORDER BY confidence DESC""", entity_id)
        if not rows:
            return "skipped_no_trusted"
        attrs = {r["attr_key"]: {"value": r["attr_value"], "confidence": r["confidence"]} for r in rows}
        hs_props = _map_to_hubspot_properties("company", attrs)
        if crm_id:
            hs_props["crm_entity_id"] = str(crm_id)
        if not hs_props:
            return "skipped_no_props"
        headers = {"Authorization": f"Bearer {HS_TOKEN}", "Content-Type": "application/json"}
        if hubspot_id:
            async with httpx.AsyncClient(timeout=httpx.Timeout(15)) as c:
                r = await c.patch(f"https://api.hubapi.com/crm/v3/objects/companies/{hubspot_id}",
                                  headers=headers, json={"properties": hs_props})
                if r.status_code in (200, 204):
                    return "updated"
                return f"error:{r.status_code}"
        elif HUBSPOT_SYNC_URL:
            async with httpx.AsyncClient(timeout=httpx.Timeout(15)) as c:
                r = await c.post(HUBSPOT_SYNC_URL, headers=headers, json={"properties": hs_props})
                if r.status_code in (200, 201, 202):
                    return f"created:{r.status_code}"
                return f"error:{r.status_code}"
        return "skipped_no_hubspot_id"
    except Exception as exc:
        log.error("HubSpot write-back error: %s", exc)
        return f"error:{exc}"
    finally:
        await conn.close()

async def write_enriched_to_crm(entity_id: str, crm_id: str | None,
                                crm_name: str = "integritasmrv") -> str:
    if not crm_id:
        return "skipped_no_crm_id"
    conn = await get_enrichment_conn()
    try:
        rows = await conn.fetch(
            """SELECT attr_key, attr_value, confidence, is_trusted
               FROM entity_attributes
               WHERE entity_id=$1::uuid AND is_trusted=true
               ORDER BY confidence DESC""", entity_id)
        if not rows:
            return "skipped_no_trusted"
        update_cols = {}
        for r in rows:
            k = r["attr_key"]
            v = r["attr_value"]
            if k in ("website", "domain", "linkedin_url", "industry", "country",
                     "company_logo_url", "description", "twitter_handle",
                     "founded_year", "sic_code", "company_email"):
                update_cols[k] = v
        if not update_cols:
            return "skipped_no_crm_fields"
        update_cols["enrichment_status"] = "Enriched Complete"
        update_cols["enrichment_score"] = float(rows[0]["confidence"]) if rows else 0
        update_cols["last_enriched_at"] = datetime.now(timezone.utc)
        update_cols["enrichment_locked_until"] = None
        set_clause = ", ".join([f"{k}=${i+2}" for i, k in enumerate(update_cols.keys())])
        sql = f"UPDATE nb_crm_customers SET {set_clause} WHERE id=${len(update_cols)+1}::bigint"
        values = list(update_cols.values()) + [int(crm_id)]
        crm_conn = await get_crm_conn(crm_name)
        try:
            await crm_conn.execute(sql, *values)
            return "updated"
        finally:
            await crm_conn.close()
    except Exception as exc:
        log.error("CRM write-back error: %s", exc)
        return f"error:{exc}"
    finally:
        await conn.close()

async def update_entity_status(entity_id: str, status: str) -> None:
    conn = await get_enrichment_conn()
    try:
        await conn.execute(
            "UPDATE entities SET enrichment_status=$2 WHERE id=$1::uuid", entity_id, status)
    finally:
        await conn.close()

async def run_enrichment_round(entity_id: str, crm_id: str | None,
                               entity_label: str, entity_type: str,
                               business_key: str, crm_name: str,
                               hubspot_id: str | None, round_n: int) -> dict:
    log.info("[%s] Round %d: entity=%s label=%s", business_key, round_n, entity_id, entity_label)

    conn = await get_enrichment_conn()
    try:
        primary = {}
        if entity_type == "company" and crm_id:
            crm_conn = await get_crm_conn(crm_name)
            try:
                row = await crm_conn.fetchrow(
                    """SELECT name, website, country, industry, company_email,
                             domain, linkedin_url, vat_number, hq_city, hq_country,
                             legal_name, trade_name, lat, lng
                       FROM nb_crm_customers WHERE id=$1::bigint""", int(crm_id))
                if row:
                    primary = dict(row)
            finally:
                await crm_conn.close()
        await conn.execute(
            "UPDATE entities SET enrichment_status='in_progress' WHERE id=$1::uuid", entity_id)
    finally:
        await conn.close()

    evidence = await run_collectors(entity_id, entity_label, primary, business_key, round_n)
    log.info("[%s] Round %d: collected %d evidence items", business_key, round_n, len(evidence))

    if evidence:
        conn = await get_enrichment_conn()
        try:
            inserted = await store_evidence(conn, evidence)
            log.info("[%s] Round %d: stored %d evidence", business_key, round_n, inserted)
        finally:
            await conn.close()

    if evidence:
        extracted = await extract_fields_llm(entity_id, round_n, business_key)
        log.info("[%s] Round %d: extracted %d fields", business_key, round_n, extracted)
        promoted = await promote_trusted_fields(entity_id)
        log.info("[%s] Round %d: promoted %d trusted fields", business_key, round_n, promoted)

    gap = await evaluate_confidence_gap(entity_id, round_n)
    log.info("[%s] Round %d: gap_eval=%s", business_key, round_n, gap)
    return gap


class EnrichmentV2Input(BaseModel):
    entity_id: str
    entity_type: str
    entity_label: str
    crm_id: str | None = None
    hubspot_id: str | None = None
    business_key: str = "integritasmrv"
    crm_name: str = "integritasmrv"


class EnrichmentV2Output(BaseModel):
    status: str
    rounds: int
    trusted_fields: int
    overall_confidence: float
    hubspot_sync: str
    crm_writeback: str


enrichment_v2_wf = hatchet.workflow(
    name="enrichment-v2-workflow",
    input_validator=EnrichmentV2Input,
    on_events=["enrichment-v2-request"],
)


@enrichment_v2_wf.task(name="enrich-company-v2")
async def enrich_company_v2(input: EnrichmentV2Input, ctx: Context) -> EnrichmentV2Output:
    log.info("[%s] Enrichment v2 started: entity=%s label=%s crm=%s",
             input.business_key, input.entity_id, input.entity_label, input.crm_name)

    round_n = 1
    gap = await run_enrichment_round(
        input.entity_id, input.crm_id, input.entity_label, input.entity_type,
        input.business_key, input.crm_name, input.hubspot_id, round_n)

    while gap["should_continue"]:
        round_n += 1
        if round_n > MAX_ROUNDS:
            break
        gap = await run_enrichment_round(
            input.entity_id, input.crm_id, input.entity_label, input.entity_type,
            input.business_key, input.crm_name, input.hubspot_id, round_n)

    final_status = "Enriched Complete" if gap["trusted_fields"] >= MIN_TRUSTED else "Enriched Partial"
    await update_entity_status(input.entity_id, final_status)

    hs_sync = await write_hubspot_trusted(
        input.entity_id, input.crm_id, input.crm_name, input.hubspot_id)

    crm_wb = await write_enriched_to_crm(input.entity_id, input.crm_id, input.crm_name)

    log.info(
        "[%s] Enrichment v2 done: entity=%s rounds=%d trusted=%d conf=%.2f hs=%s crm=%s",
        input.business_key, input.entity_id, round_n, gap["trusted_fields"],
        gap["overall_confidence"], hs_sync, crm_wb)

    return EnrichmentV2Output(
        status=final_status,
        rounds=round_n,
        trusted_fields=gap["trusted_fields"],
        overall_confidence=gap["overall_confidence"],
        hubspot_sync=hs_sync,
        crm_writeback=crm_wb,
    )


class ContactEnrichmentV2Input(BaseModel):
    entity_id: str
    entity_type: str
    entity_label: str
    crm_id: str | None = None
    hubspot_id: str | None = None
    email: str | None = None
    phone: str | None = None
    title: str | None = None
    linkedin: str | None = None
    business_key: str = "integritasmrv"
    crm_name: str = "integritasmrv"


class ContactEnrichmentV2Output(BaseModel):
    status: str
    trusted_fields: int
    overall_confidence: float
    hubspot_sync: str
    crm_writeback: str


contact_enrichment_v2_wf = hatchet.workflow(
    name="contact-enrichment-v2-workflow",
    input_validator=ContactEnrichmentV2Input,
    on_events=["contact-enrichment-v2-request"],
)


@contact_enrichment_v2_wf.task(name="enrich-contact-v2")
async def enrich_contact_v2(input: ContactEnrichmentV2Input, ctx: Context) -> ContactEnrichmentV2Output:
    log.info("[%s] Contact enrichment v2 started: entity=%s label=%s",
             input.business_key, input.entity_id, input.entity_label)

    round_n = 1
    await update_entity_status(input.entity_id, "in_progress")

    gap = await run_enrichment_round(
        input.entity_id, input.crm_id, input.entity_label, input.entity_type,
        input.business_key, input.crm_name, input.hubspot_id, round_n)

    while gap["should_continue"]:
        round_n += 1
        if round_n > MAX_ROUNDS:
            break
        gap = await run_enrichment_round(
            input.entity_id, input.crm_id, input.entity_label, input.entity_type,
            input.business_key, input.crm_name, input.hubspot_id, round_n)

    final_status = "Enriched Complete" if gap["trusted_fields"] >= MIN_TRUSTED else "Enriched Partial"
    await update_entity_status(input.entity_id, final_status)

    hs_sync = await write_hubspot_trusted(
        input.entity_id, input.crm_id, input.crm_name, input.hubspot_id)

    log.info(
        "[%s] Contact enrichment v2 done: entity=%s rounds=%d trusted=%d conf=%.2f hs=%s",
        input.business_key, input.entity_id, round_n, gap["trusted_fields"],
        gap["overall_confidence"], hs_sync)

    return ContactEnrichmentV2Output(
        status=final_status,
        trusted_fields=gap["trusted_fields"],
        overall_confidence=gap["overall_confidence"],
        hubspot_sync=hs_sync,
        crm_writeback="skipped",
    )


worker = hatchet.worker("hatchet-worker-v2",
    workflows=[enrichment_v2_wf, contact_enrichment_v2_wf])

if __name__ == "__main__":
    log.info("Starting Hatchet worker v2...")
    worker.start()

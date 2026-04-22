
# === HEURISTIC EXTRACTION ===
def _heuristic_fields_from_evidence(
    evidence_rows: list, entity_label: str, seeded_website: str | None
) -> dict[tuple[str, str], tuple[float, str]]:
    out: dict[tuple[str, str], tuple[float, str]] = {}
    domains: dict[str, int] = {}
    linkedin_sources: dict[str, float] = {}
    entity_tokens = _entity_tokens(entity_label)
    entity_slug = _company_slug_from_entity_label(entity_label)
    seeded_domain = urlparse((seeded_website or "")).netloc.lower().replace("www.", "") if seeded_website else ""

    for e in evidence_rows:
        src_url = str(e.get("source_url") or "").strip()
        if not src_url:
            continue
        domain = urlparse(src_url).netloc.lower().replace("www.", "")
        if domain:
            domains[domain] = domains.get(domain, 0) + 1
        normalized_linkedin = _normalize_linkedin_company_url(src_url)
        if normalized_linkedin:
            linkedin_sources[normalized_linkedin] = linkedin_sources.get(normalized_linkedin, 0.0) + 2.0
        for c in _extract_linkedin_company_urls(str(e.get("raw_content") or "")):
            if c not in linkedin_sources:
                linkedin_sources[c] = linkedin_sources.get(c, 0.0) + 2.0

    if linkedin_sources:
        scored: list[tuple[float, str]] = []
        for u, base_score in linkedin_sources.items():
            lowered = u.lower()
            score = base_score + 10.0
            if "/life" not in lowered and "/jobs" not in lowered:
                score += 5
            if any(tok in lowered for tok in entity_tokens):
                score += 6
            if "/group" in lowered or "/holding" in lowered:
                score += 2
            slug = lowered.split("/company/")[-1].split("/")[0]
            if entity_slug and slug == entity_slug:
                score += 14
            score -= _slug_branch_penalty(slug, entity_label)
            scored.append((score, u))
        scored.sort(key=lambda x: x[0], reverse=True)
        if scored:
            primary = scored[0][1]
            out[("linkedin_company_url", primary)] = (0.95, "heuristic_linkedin_primary")
            out[("linkedin_company_urls", json.dumps(list(linkedin_sources.keys())))] = (0.9, "heuristic_linkedin_set")

    if seeded_website and seeded_website.startswith(("http://", "https://")):
        out[("official_website", seeded_website)] = (0.97, "heuristic_seed_website")

    if domains and not any(k == "official_website" for (k, _v) in out.keys()):
        candidates = []
        for dom, freq in domains.items():
            if _domain_is_directory(dom):
                continue
            score = float(freq)
            if any(tok in dom for tok in entity_tokens):
                score += 2.5
            candidates.append((score, dom))
        if candidates:
            candidates.sort(key=lambda x: x[0], reverse=True)
            best_domain = candidates[0][1]
            out[("official_website", f"https://{best_domain}")] = (0.92, "heuristic_domain")

    for e in evidence_rows:
        text = str(e.get("raw_content") or "")[:1200]
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        for ln in lines:
            if ln.startswith(("http://", "https://")):
                continue
            if len(ln) < 40:
                continue
            if re.search(r"^[\-*#]+\s*$", ln):
                continue
            out[("company_description", ln[:600])] = (0.62, "heuristic_text")
            break
        if any(k == "company_description" for (k, _v) in out.keys()):
            break

    for e in evidence_rows:
        collector = str(e.get("collector_name") or "").lower()
        raw = str(e.get("raw_content") or "")
        if not raw:
            continue
        if "nominatim" in collector:
            try:
                payload = json.loads(raw)
                lat = _normalize_field_value("lat", payload.get("lat"))
                lng = _normalize_field_value("lng", payload.get("lng"))
                addr = payload.get("address") or {}
                road = str(addr.get("road") or "").strip()
                house = str(addr.get("house_number") or "").strip()
                full_addr = (road + " " + house).strip()
                city = _normalize_field_value("city", addr.get("city"))
                postcode = _normalize_field_value("postcode", addr.get("postcode"))
                country = _normalize_field_value("country", addr.get("country"))
                cc = _normalize_field_value("country_code", addr.get("country_code"))
                if lat: out[("lat", lat)] = (0.97, "nominatim_heuristic")
                if lng: out[("lng", lng)] = (0.97, "nominatim_heuristic")
                if full_addr: out[("address", full_addr)] = (0.92, "nominatim_heuristic")
                if city: out[("city", city)] = (0.93, "nominatim_heuristic")
                if postcode: out[("postcode", postcode)] = (0.9, "nominatim_heuristic")
                if country: out[("country", country)] = (0.92, "nominatim_heuristic")
                if cc: out[("country_code", cc)] = (0.96, "nominatim_heuristic")
            except Exception:
                pass
        if "overpass" in collector:
            try:
                payload = json.loads(raw)
                rows = payload.get("results") if isinstance(payload, dict) else []
                if not isinstance(rows, list):
                    rows = []
                for row in rows[:5]:
                    phone = _normalize_field_value("phone", row.get("phone"))
                    website = _normalize_field_value("website", row.get("website"))
                    if phone: out[("phone", phone)] = (0.85, "overpass_heuristic")
                    if website: out[("website", website)] = (0.84, "overpass_heuristic")
            except Exception:
                pass
    return out


# === LLM EXTRACTION ===
async def _create_standard_summaries(conn: asyncpg.Connection, entity_id: str,
                                      entity_type: str, round_number: int,
                                      evidence_rows: list, context: str,
                                      business_key: str) -> None:
    await conn.execute(
        """CREATE TABLE IF NOT EXISTS entity_summaries (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(), entity_id UUID NOT NULL,
            entity_type TEXT NOT NULL, round_number INTEGER NOT NULL,
            summary_text TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE (entity_id, round_number))""")
    await conn.execute(
        """CREATE TABLE IF NOT EXISTS raw_evidence_summaries (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(), raw_evidence_id UUID NOT NULL,
            entity_id UUID NOT NULL, summary_kind TEXT NOT NULL,
            round_number INTEGER NOT NULL, summary_text TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (raw_evidence_id, round_number))""")

    prompt = (f"Summarize this {entity_type} using evidence only. "
              "Return plain text only, max 1000 characters, no markdown. "
              "Facts only from provided evidence.\n\nEvidence:\n" + context[:9000])
    try:
        entity_summary = await llm_complete("summarizer", prompt, max_tokens=450)
    except Exception:
        entity_summary = context
    entity_summary = _compact_summary(entity_summary, max_chars=1000)
    if entity_summary:
        await conn.execute(
            """INSERT INTO entity_summaries (entity_id, entity_type, round_number, summary_text)
               VALUES ($1::uuid, $2, $3, $4)
               ON CONFLICT (entity_id, round_number)
               DO UPDATE SET entity_type=EXCLUDED.entity_type, summary_text=EXCLUDED.summary_text, updated_at=NOW()""",
            entity_id, entity_type, round_number, entity_summary)
        try:
            await lightrag_insert(entity_summary, business_key)
            ev_vec = await llm_embed(entity_summary)
            if ev_vec:
                await qdrant_upsert("intelligence_data", entity_id, ev_vec,
                                    {"entity_type": entity_type, "round_number": round_number, "kind": "entity_summary"}, business_key)
        except Exception as exc:
            log.warning("lightrag/qdrant insert failed: %s", exc)


async def extract_fields_llm(entity_id: str, round_number: int, business_key: str = "integritasmrv") -> int:
    conn = await get_enrichment_conn()
    try:
        evidence = await conn.fetch(
            """SELECT id, collector_name, source_url, content_type, raw_content
               FROM raw_evidence WHERE entity_id=$1 ORDER BY scraped_at DESC LIMIT 40""", entity_id)
        entity_type = await conn.fetchval("SELECT entity_type FROM entities WHERE id=$1::uuid", entity_id) or "generic"
        entity_label = await conn.fetchval("SELECT label FROM entities WHERE id=$1::uuid", entity_id) or ""
        seeded_website = await conn.fetchval(
            """SELECT attr_value FROM entity_attributes
               WHERE entity_id=$1::uuid AND attr_key='website'
               ORDER BY confidence DESC, updated_at DESC LIMIT 1""", entity_id)
        context = _build_evidence_context([dict(r) for r in evidence])
        if not context:
            return 0

        await _create_standard_summaries(conn, entity_id, str(entity_type), round_number,
                                          [dict(r) for r in evidence], context, business_key)

        extracted: dict[tuple[str, str], tuple[float, str]] = {}
        keys_str = ", ".join(COMPANY_EXTRACT_FIELDS)
        prompt = (f"Extract only these fields from evidence: {keys_str}.\n"
                  "Rules:\n- return facts only when supported by evidence\n"
                  "- confidence 0.0-1.0\n- if value is missing, omit field\n"
                  "Return JSON: {\"fields\":[{\"key\":\"...\",\"value\":\"...\",\"confidence\":0.0,\"source_collector\":\"...\"}]}\n"
                  f"Evidence:\n{context}\n")
        try:
            raw = await llm_complete("extractor", prompt, json_mode=True, max_tokens=1400)
            parsed = json.loads(raw)
        except Exception as exc:
            log.warning("LLM extraction failed for %s: %s", entity_id, exc)
            parsed = {}

        fields = parsed.get("fields", []) if isinstance(parsed, dict) else []
        for f in fields:
            key = str(f.get("key") or "").strip()
            if key not in COMPANY_EXTRACT_FIELDS:
                continue
            val = _normalize_field_value(key, f.get("value"))
            if val is None:
                continue
            conf = max(0.0, min(1.0, float(f.get("confidence") or 0.0)))
            src = str(f.get("source_collector") or "llm_extractor").strip()
            conf = _calibrate_confidence(key, val, conf, src)
            id_key = (key, val)
            prev = extracted.get(id_key)
            if prev is None or conf > prev[0]:
                extracted[id_key] = (conf, src)

        for id_key, data in _heuristic_fields_from_evidence(
                [dict(r) for r in evidence], entity_label=entity_label,
                seeded_website=seeded_website).items():
            if id_key not in extracted:
                k, v = id_key
                c, s = data
                extracted[id_key] = (_calibrate_confidence(k, v, c, s), s)

        BLOCKED = {"opening_hours", "business_hours", "hours_of_operation"}
        inserted = 0
        for (key, val), (conf, src) in extracted.items():
            if key in BLOCKED:
                continue
            try:
                if key == "linkedin_company_urls":
                    result = await conn.execute(
                        """INSERT INTO entity_attributes
                           (entity_id, attr_key, attr_value, attr_value_json, confidence,
                            round_number, extraction_method, source_collector, llm_alias_used)
                           SELECT $1::uuid, $2, NULL, $3::jsonb, $4, $5, 'llm_field_specific', $6, 'extractor'
                           WHERE NOT EXISTS (
                               SELECT 1 FROM entity_attributes
                               WHERE entity_id=$1 AND attr_key=$2)""",
                        entity_id, key, val, conf, round_number, src)
                else:
                    result = await conn.execute(
                        """INSERT INTO entity_attributes
                           (entity_id, attr_key, attr_value, confidence,
                            round_number, extraction_method, source_collector, llm_alias_used)
                           SELECT $1::uuid, $2, $3, $4, $5, 'llm_field_specific', $6, 'extractor'
                           WHERE NOT EXISTS (
                               SELECT 1 FROM entity_attributes
                               WHERE entity_id=$1 AND attr_key=$2 AND attr_value=$3)""",
                        entity_id, key, val, conf, round_number, src)
                if str(result).endswith("1"):
                    inserted += 1
            except Exception as exc:
                log.warning("insert attribute failed: %s", exc)
        return inserted
    finally:
        await conn.close()


# === TRUSTED FIELDS ===
async def promote_trusted_fields(entity_id: str) -> int:
    conn = await get_enrichment_conn()
    try:
        result = await conn.execute(
            """UPDATE entity_attributes ea
               SET is_trusted=true, updated_at=NOW()
               WHERE ea.entity_id=$1 AND ea.is_trusted=false
                 AND (
                   (SELECT COUNT(DISTINCT source_collector)
                    FROM entity_attributes ea2
                    WHERE ea2.entity_id=ea.entity_id
                      AND ea2.attr_key=ea.attr_key
                      AND ea2.attr_value=ea.attr_value) >= 2
                   OR ea.confidence >= 0.95
                   OR (ea.attr_key='linkedin_company_url' AND ea.confidence >= 0.9)
                   OR (ea.attr_key='linkedin_company_urls' AND ea.confidence >= 0.85)
                 )""", entity_id)
        return int(result.split()[-1])
    finally:
        await conn.close()


async def evaluate_confidence_gap(entity_id: str, round_number: int) -> dict:
    conn = await get_enrichment_conn()
    try:
        row = await conn.fetchrow(
            """WITH best AS (
                SELECT attr_key, MAX(confidence) AS best_conf,
                       BOOL_OR(is_trusted) AS trusted
                FROM entity_attributes WHERE entity_id=$1::uuid GROUP BY attr_key)
                SELECT
                    COALESCE(COUNT(*) FILTER (WHERE trusted), 0)::int AS trusted_fields,
                    COALESCE(AVG(best_conf), 0.0)::float AS overall_confidence
                FROM best""", entity_id)
        trusted_fields = int(row["trusted_fields"] if row else 0)
        overall_confidence = float(row["overall_confidence"] if row else 0.0)
        await conn.execute(
            "UPDATE entities SET enrichment_round=$2, overall_confidence=$3 WHERE id=$1::uuid",
            entity_id, round_number, overall_confidence)
        should_continue = (
            trusted_fields < MIN_TRUSTED or
            overall_confidence < MIN_CONF
        ) and round_number < MAX_ROUNDS
        return {
            "should_continue": should_continue,
            "round": round_number,
            "trusted_fields": trusted_fields,
            "overall_confidence": overall_confidence,
            "min_trusted_fields": MIN_TRUSTED,
            "min_overall_confidence": MIN_CONF,
        }
    finally:
        await conn.close()


# === HUBSPOT WRITE BACK ===
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
        attrs = {r["attr_key"]: {"value": r["attr_value"], "confidence": r["confidence"]}
                 for r in rows}
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


# === CRM WRITE BACK ===
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


# === UPDATE ENRICHMENT STATUS ===
async def update_entity_status(entity_id: str, status: str) -> None:
    conn = await get_enrichment_conn()
    try:
        await conn.execute(
            "UPDATE entities SET enrichment_status=$2 WHERE id=$1::uuid",
            entity_id, status)
    finally:
        await conn.close()


# === MAIN ENRICHMENT LOOP ===
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
            "UPDATE entities SET enrichment_status='in_progress' WHERE id=$1::uuid",
            entity_id)
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


# === PYDANTIC MODELS ===
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


# === WORKFLOW DEFINITIONS ===
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

    crm_wb = "skipped"  # contact writeback can be added similarly

    log.info(
        "[%s] Contact enrichment v2 done: entity=%s rounds=%d trusted=%d conf=%.2f hs=%s",
        input.business_key, input.entity_id, round_n, gap["trusted_fields"],
        gap["overall_confidence"], hs_sync)

    return ContactEnrichmentV2Output(
        status=final_status,
        trusted_fields=gap["trusted_fields"],
        overall_confidence=gap["overall_confidence"],
        hubspot_sync=hs_sync,
        crm_writeback=crm_wb,
    )


worker = hatchet.worker("hatchet-worker-v2",
    workflows=[enrichment_v2_wf, contact_enrichment_v2_wf])

if __name__ == "__main__":
    log.info("Starting Hatchet worker v2...")
    worker.start()

import os
import json
import re
import logging

import httpx
import asyncpg
from hatchet_sdk import Hatchet, Context
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [worker] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

T = os.environ.get(
    "HATCHET_CLIENT_TOKEN",
    "eyJhbGciOiJFUzI1NiIsImtpZCI6IkRFOWxydyJ9.eyJhdWQiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCJleHAiOjE3ODQzMTI1MzQsImdycGNfYnJvYWRjYXN0X2FkZHJlc3MiOiJoYXRjaGV0LWVuZ2luZTo3MDcwIiwiaWF0IjoxNzc2NTM2NTM0LCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCJzZXJ2ZXJfdXJsIjoiaHR0cDovL2xvY2FsaG9zdDo4MDgwIiwic3ViIjoiNzA3ZDA4NTUtODBhYi00ZTFmLWExNTYtZjFjNDU0NmNiZjUyIiwidG9rZW5faWQiOiI1Y2NhNTU1MS03ODYwLTQxYTQtODMzZC1lNTg0NTQ3YTM4MjAifQ.V6kV3M5OZB5xHhjtrIOCEs_rif78GhW5_yno6q9qnJgO4dCRnqY8UAgERVert3XYmgv5sf_g7_hhq_xjoDpisw",
)
os.environ["HATCHET_CLIENT_TOKEN"] = T
os.environ["HATCHET_CLIENT_HOST_PORT"] = os.environ.get("HATCHET_CLIENT_HOST_PORT", "127.0.0.1:7070")
os.environ["HATCHET_CLIENT_TLS_STRATEGY"] = "none"

hatchet = Hatchet()

SEARXNG_URL = os.environ.get("SEARXNG_URL", "http://144.91.126.111:3010")
CRM_DSN = os.environ.get(
    "CRM_DSN",
    "postgresql://integritasmrv_crm_user:oYxxPKRfAHAD263VSDcKmljKY0vInx2QTl6PooKoqmmiDops@crm-integritasmrv-db:5432/integritasmrv_crm",
)
HS_TOKEN = os.environ.get("HUBSPOT_API_TOKEN", "")


async def get_crm_conn():
    return await asyncpg.connect(CRM_DSN)


async def claim_company(company_id: str) -> dict | None:
    conn = await get_crm_conn()
    try:
        row = await conn.fetchrow(
            """
            UPDATE nb_crm_customers
            SET enrichment_status='Enrichment Busy',
                enrichment_locked_until=NOW() + INTERVAL '30 minutes',
                enrichment_worker_id='hatchet-worker'
            WHERE id = $1::bigint
              AND enrichment_status IN ('To Be Enriched', 'Enrichment Failed')
            RETURNING id, name, website, country, industry, hubspot_id,
                      company_email, domain, linkedin_url, vat_number
            """,
            int(company_id),
        )
        return dict(row) if row else None
    finally:
        await conn.close()


async def write_crm(company_id: str, enriched: dict):
    conn = await get_crm_conn()
    try:
        sets = []
        values = []
        idx = 1
        field_map = {
            "website": "website",
            "domain": "domain",
            "linkedin_url": "linkedin_url",
            "industry": "industry",
            "country": "country",
            "company_logo_url": "company_logo_url",
            "description": "description",
            "twitter_handle": "twitter_handle",
            "founded_year": "founded_year",
            "sic_code": "sic_code",
            "company_email": "company_email",
        }
        for col, key in field_map.items():
            val = enriched.get(key)
            if val is not None:
                idx += 1
                sets.append(f"{col} = ${idx}")
                values.append(val)

        idx += 1
        sets.append(f"enrichment_status = ${idx}")
        values.append(enriched.get("status", "Enriched Partial"))

        idx += 1
        sets.append(f"enrichment_score = ${idx}")
        values.append(enriched.get("score", 0))

        idx += 1
        sets.append(f"enrichment_notes = ${idx}")
        values.append(enriched.get("notes", ""))

        idx += 1
        sets.append(f"enrichment_source = ${idx}::jsonb")
        values.append(json.dumps(enriched.get("sources", [])))

        idx += 1
        sets.append(f"last_enriched_at = NOW()")

        idx += 1
        sets.append(f"enrichment_locked_until = ${idx}")
        values.append(None)

        idx += 1
        sets.append(f"enrichment_worker_id = ${idx}")
        values.append(None)

        idx += 1
        sets.append(f"last_enrichment_run_id = ${idx}")
        values.append(enriched.get("run_id"))

        values.append(int(company_id))
        where_idx = idx + 1
        sql = f"UPDATE nb_crm_customers SET {', '.join(sets)} WHERE id = ${where_idx}"
        await conn.execute(sql, *values)
        log.info("Wrote enriched data to CRM: id=%s", company_id)
    finally:
        await conn.close()


async def write_hubspot(hubspot_id: str, enriched: dict, crm_id: str):
    if not HS_TOKEN or not hubspot_id:
        log.warning("HubSpot write-back skipped: token=%s hubspot_id=%s", bool(HS_TOKEN), hubspot_id)
        return "skipped"

    headers = {"Authorization": f"Bearer {HS_TOKEN}", "Content-Type": "application/json"}
    props = {"enrichment_status_c": enriched.get("status", "")}
    if enriched.get("website"):
        props["website"] = enriched["website"]
    if enriched.get("industry"):
        props["industry"] = enriched["industry"]
    if enriched.get("linkedin_url"):
        props["linkedin_company_page"] = enriched["linkedin_url"]
    if crm_id:
        props["crm_entity_id"] = str(crm_id)
    if enriched.get("company_logo_url"):
        props["company_logo_url"] = enriched["company_logo_url"]
    if enriched.get("domain"):
        props["domain"] = enriched["domain"]

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15)) as c:
            r = await c.patch(
                f"https://api.hubapi.com/crm/v3/objects/companies/{hubspot_id}",
                headers=headers,
                json={"properties": props},
            )
            if r.status_code in (200, 204):
                log.info("Updated HubSpot company: hubspot_id=%s", hubspot_id)
                return "updated"
            log.error("HubSpot update failed: %s %s", r.status_code, r.text[:200])
            return f"error:{r.status_code}"
    except Exception as exc:
        log.error("HubSpot write-back error: %s", exc)
        return f"error:{exc}"


async def searxng_enrich(name: str, country: str | None, existing_website: str | None) -> dict:
    sources = []
    urls = []
    try:
        params = {"q": f'"{name}" company {country or ""} official website'}
        async with httpx.AsyncClient(timeout=httpx.Timeout(12)) as c:
            r = await c.get(f"{SEARXNG_URL}/search", params=params)
            r.raise_for_status()
            urls = re.findall(r'href="(https?://[^"#]+)"', r.text)
        sources = [u for u in dict.fromkeys(urls) if "144.91.126" not in u][:5]
    except Exception as exc:
        log.error("SEARXNG search error: %s", exc)

    enriched_website = existing_website
    if not enriched_website and sources:
        for u in sources:
            low = u.lower()
            if any(t in low for t in ["linkedin.com", "facebook.com", "twitter.com", "wikipedia"]):
                continue
            enriched_website = u.rstrip("/")
            break

    domain = None
    if enriched_website:
        from urllib.parse import urlparse
        domain = urlparse(enriched_website).hostname

    linkedin_url = None
    for u in sources:
        if "linkedin.com/company" in u.lower():
            linkedin_url = u
            break

    score = 0
    if existing_website or enriched_website:
        score += 40
    if country:
        score += 10
    score += min(40, len(sources) * 8)
    if linkedin_url:
        score += 10

    status = "Enriched Complete" if score >= 80 and len(sources) >= 3 else "Enriched Partial"

    return {
        "website": enriched_website,
        "domain": domain,
        "linkedin_url": linkedin_url,
        "sources": sources,
        "score": min(score, 100),
        "status": status,
        "notes": f"urls={len(sources)} linkedin={'yes' if linkedin_url else 'no'} domain={'yes' if domain else 'no'}",
    }


class EnrichInput(BaseModel):
    company_id: str
    name: str
    website: str | None = None
    country: str | None = None
    industry: str | None = None
    hubspot_id: str | None = None
    company_email: str | None = None


class EnrichOutput(BaseModel):
    status: str
    score: int
    notes: str
    sources: list[str]
    hubspot_sync: str


enr_wf = hatchet.workflow(name="enrichment-workflow", input_validator=EnrichInput, on_events=["enrichment-request"])


@enr_wf.task(name="enrich-company")
async def enrich_company(input: EnrichInput, ctx: Context) -> EnrichOutput:
    log.info("Enrichment started: id=%s name=%s", input.company_id, input.name)

    claimed = await claim_company(input.company_id)
    if not claimed:
        log.warning("Could not claim company id=%s — may already be busy", input.company_id)
        return EnrichOutput(status="skipped", score=0, notes="already_busy", sources=[], hubspot_sync="skipped")

    enriched = await searxng_enrich(claimed["name"], claimed.get("country"), claimed.get("website"))
    enriched["run_id"] = ctx.workflow_run_id

    await write_crm(input.company_id, enriched)

    hubspot_sync = await write_hubspot(
        claimed.get("hubspot_id") or input.hubspot_id,
        enriched,
        input.company_id,
    )

    log.info(
        "Enrichment complete: id=%s status=%s score=%s hs=%s",
        input.company_id, enriched["status"], enriched["score"], hubspot_sync,
    )
    return EnrichOutput(
        status=enriched["status"],
        score=enriched["score"],
        notes=enriched["notes"],
        sources=enriched["sources"],
        hubspot_sync=hubspot_sync,
    )


class LeadInput(BaseModel):
    message: str

class LeadOutput(BaseModel):
    result: str

cf7_wf = hatchet.workflow(name="cf7-to-crm-workflow", input_validator=LeadInput, on_events=["cf7-lead"])

@cf7_wf.task(name="process-lead")
async def process_lead(input: LeadInput, ctx: Context) -> LeadOutput:
    log.info("CF7 lead: %s", input.message)
    return LeadOutput(result=f"Processed: {input.message}")


class HubSpotInput(BaseModel):
    email: str
    firstname: str | None = None
    lastname: str | None = None
    company: str | None = None
    phone: str | None = None

class HubSpotOutput(BaseModel):
    hubspot_id: str | None = None
    status: str

hs_wf = hatchet.workflow(name="hubspot-sync", input_validator=HubSpotInput, on_events=["hubspot-sync"])

@hs_wf.task(name="sync-contact")
async def sync_contact(input: HubSpotInput, ctx: Context) -> HubSpotOutput:
    if not HS_TOKEN:
        return HubSpotOutput(status="skipped_no_token")
    headers = {"Authorization": f"Bearer {HS_TOKEN}", "Content-Type": "application/json"}
    props = {"email": input.email}
    for k in ["firstname", "lastname", "company", "phone"]:
        v = getattr(input, k, None)
        if v:
            props[k] = v
    async with httpx.AsyncClient(timeout=httpx.Timeout(15)) as c:
        r = await c.post("https://api.hubapi.com/crm/v3/objects/contacts", headers=headers, json={"properties": props})
        if r.status_code in (200, 201):
            return HubSpotOutput(hubspot_id=r.json().get("id"), status="created")
        return HubSpotOutput(status=f"error:{r.status_code}")


worker = hatchet.worker("integritas-worker", workflows=[cf7_wf, enr_wf, hs_wf])

if __name__ == "__main__":
    log.info("Starting Hatchet worker...")
    worker.start()

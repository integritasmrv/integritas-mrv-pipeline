import os
import json
import re
import logging

import httpx
import asyncpg
from hatchet_sdk import Hatchet, Context
from pydantic import BaseModel
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [worker] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

T = os.environ.get(
    "HATCHET_CLIENT_TOKEN",
    "eyJhbGciOiJFUzI1NiIsImtpZCI6ImZ5cTd3QSJ9.eyJhdWQiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCJleHAiOjE3ODQzNjU5MTUsImdycGNfYnJvYWRjYXN0X2FkZHJlc3MiOiJoYXRjaGV0LWVuZ2luZTo3MDcwIiwiaWF0IjoxNzc2NTg5OTE1LCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCJzZXJ2ZXJfdXJsIjoiaHR0cDovL2xvY2FsaG9zdDo4MDgwIiwic3ViIjoiNzA3ZDA4NTUtODBhYi00ZTFmLWExNTYtZjFjNDU0NmNiZjUyIiwidG9rZW5faWQiOiJiMDc5MTc4Zi02OGQ4LTRhMzQtYThlYy1kNzQ2YjE4OGZkMzcifQ.FKSllU33io3P3Dl4bsWvdxPQzHCnXmKpU3PK_vx27DhQvaYjMmFzB91UT2Jw82mvcolXtNBK9tSNP_KcabHCKw",
)
os.environ["HATCHET_CLIENT_TOKEN"] = T
os.environ["HATCHET_CLIENT_HOST_PORT"] = os.environ.get("HATCHET_CLIENT_HOST_PORT", "127.0.0.1:7070")
os.environ["HATCHET_CLIENT_TLS_STRATEGY"] = "none"

hatchet = Hatchet()

SEARXNG_URL = os.environ.get("SEARXNG_URL", "http://144.91.126.111:3010")
HS_TOKEN = os.environ.get("HUBSPOT_API_TOKEN", "")

CRM_DBS = {
    "integritasmrv": {
        "host": os.environ.get("CRM_INTEGRITASMRV_HOST", "127.0.0.1"),
        "port": int(os.environ.get("CRM_INTEGRITASMRV_PORT", "15432")),
        "user": os.environ.get("CRM_INTEGRITASMRV_USER", "integritasmrv_crm_user"),
        "password": os.environ.get("CRM_INTEGRITASMRV_PASS", "oYxxPKRfAHAD263VSDcKmljKY0vInx2QTl6PooKoqmmiDops"),
        "database": os.environ.get("CRM_INTEGRITASMRV_DB", "integritasmrv_crm"),
    },
    "poweriq": {
        "host": os.environ.get("CRM_POWERIQ_HOST", "127.0.0.1"),
        "port": int(os.environ.get("CRM_POWERIQ_PORT", "15433")),
        "user": os.environ.get("CRM_POWERIQ_USER", "poweriq_crm_user"),
        "password": os.environ.get("CRM_POWERIQ_PASS", "P0w3r1Q_CRM_S3cur3_P@ss_2026"),
        "database": os.environ.get("CRM_POWERIQ_DB", "poweriq_crm"),
    },
}


async def get_crm_conn(crm_name: str = "integritasmrv"):
    cfg = CRM_DBS.get(crm_name, CRM_DBS["integritasmrv"])
    return await asyncpg.connect(
        host=cfg["host"], port=cfg["port"],
        user=cfg["user"], password=cfg["password"],
        database=cfg["database"], timeout=30,
    )


async def claim_company(company_id: str, crm_name: str = "integritasmrv") -> dict | None:
    conn = await get_crm_conn(crm_name)
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


async def claim_contact(contact_id: str, crm_name: str = "integritasmrv") -> dict | None:
    conn = await get_crm_conn(crm_name)
    try:
        row = await conn.fetchrow(
            """
            UPDATE nb_crm_contacts
            SET enrichment_status='Enrichment Busy',
                enrichment_locked_until=NOW() + INTERVAL '30 minutes',
                enrichment_worker_id='hatchet-worker'
            WHERE id = $1::bigint
              AND enrichment_status IN ('To Be Enriched', 'Enrichment Failed')
            RETURNING id, name, email, phone, title, linkedin, hubspot_id, customer_id
            """,
            int(contact_id),
        )
        return dict(row) if row else None
    finally:
        await conn.close()


async def write_crm_company(company_id: str, enriched: dict, crm_name: str = "integritasmrv"):
    conn = await get_crm_conn(crm_name)
    try:
        sets = ["website = $1", "domain = $2", "linkedin_url = $3",
                "industry = $4", "country = $5", "company_logo_url = $6",
                "description = $7", "twitter_handle = $8", "founded_year = $9",
                "sic_code = $10", "company_email = $11",
                "enrichment_status = $12", "enrichment_score = $13",
                "enrichment_notes = $14", "enrichment_source = $15::jsonb",
                "last_enriched_at = NOW()", "enrichment_locked_until = NULL",
                "enrichment_worker_id = NULL", "last_enrichment_run_id = $16"]
        values = [
            enriched.get("website"), enriched.get("domain"),
            enriched.get("linkedin_url"), enriched.get("industry"),
            enriched.get("country"), enriched.get("company_logo_url"),
            enriched.get("description"), enriched.get("twitter_handle"),
            enriched.get("founded_year"), enriched.get("sic_code"),
            enriched.get("company_email"),
            enriched.get("status", "Enriched Partial"),
            enriched.get("score", 0), enriched.get("notes", ""),
            json.dumps(enriched.get("sources", [])),
            enriched.get("run_id"), int(company_id),
        ]
        sql = f"UPDATE nb_crm_customers SET {', '.join(sets)} WHERE id = $17"
        await conn.execute(sql, *values)
        log.info("[%s] Wrote enriched company: id=%s", crm_name, company_id)
    finally:
        await conn.close()


async def write_crm_contact(contact_id: str, enriched: dict, crm_name: str = "integritasmrv"):
    conn = await get_crm_conn(crm_name)
    try:
        sets = [
            "title = $1", "linkedin = $2",
            "enrichment_status = $3", "enrichment_score = $4",
            "enrichment_notes = $5", "enrichment_source = $6::jsonb",
            "last_enriched_at = NOW()", "enrichment_locked_until = NULL",
            "enrichment_worker_id = NULL", "last_enrichment_run_id = $7",
        ]
        values = [
            enriched.get("title"),
            enriched.get("linkedin"),
            enriched.get("status", "Enriched Partial"),
            enriched.get("score", 0),
            enriched.get("notes", ""),
            json.dumps(enriched.get("sources", [])),
            enriched.get("run_id"),
            int(contact_id),
        ]
        sql = f"UPDATE nb_crm_contacts SET {', '.join(sets)} WHERE id = $8"
        await conn.execute(sql, *values)
        log.info("[%s] Wrote enriched contact: id=%s", crm_name, contact_id)
    finally:
        await conn.close()


async def write_hubspot_company(hubspot_id: str, enriched: dict, crm_id: str):
    if not HS_TOKEN or not hubspot_id:
        log.warning("HubSpot company write-back skipped: token=%s hubspot_id=%s", bool(HS_TOKEN), hubspot_id)
        return "skipped"

    headers = {"Authorization": f"Bearer {HS_TOKEN}", "Content-Type": "application/json"}
    props = {}
    if enriched.get("website"):
        props["website"] = enriched["website"]
    if enriched.get("industry"):
        props["industry"] = enriched["industry"]
    if enriched.get("linkedin_url"):
        props["linkedin_company_page"] = enriched["linkedin_url"]
    if enriched.get("company_logo_url"):
        props["company_logo_url"] = enriched["company_logo_url"]
    if enriched.get("domain"):
        props["domain"] = enriched["domain"]

    if not props:
        return "skipped_no_props"

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
            log.error("HubSpot company update failed: %s %s", r.status_code, r.text[:200])
            return f"error:{r.status_code}"
    except Exception as exc:
        log.error("HubSpot company write-back error: %s", exc)
        return f"error:{exc}"


async def write_hubspot_contact(hubspot_id: str, enriched: dict, crm_id: str):
    if not HS_TOKEN or not hubspot_id:
        log.warning("HubSpot contact write-back skipped: token=%s hubspot_id=%s", bool(HS_TOKEN), hubspot_id)
        return "skipped"

    headers = {"Authorization": f"Bearer {HS_TOKEN}", "Content-Type": "application/json"}
    props = {}
    if enriched.get("linkedin"):
        props["linkedin_company_page"] = enriched["linkedin"]
    if enriched.get("title"):
        props["jobtitle"] = enriched["title"]

    if not props:
        return "skipped_no_props"

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15)) as c:
            r = await c.patch(
                f"https://api.hubapi.com/crm/v3/objects/contacts/{hubspot_id}",
                headers=headers,
                json={"properties": props},
            )
            if r.status_code in (200, 204):
                log.info("Updated HubSpot contact: hubspot_id=%s", hubspot_id)
                return "updated"
            log.error("HubSpot contact update failed: %s %s", r.status_code, r.text[:200])
            return f"error:{r.status_code}"
    except Exception as exc:
        log.error("HubSpot contact write-back error: %s", exc)
        return f"error:{exc}"


async def searxng_enrich_company(name: str, country: str | None, existing_website: str | None) -> dict:
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
        log.error("SEARXNG company search error: %s", exc)

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


async def searxng_enrich_contact(name: str, email: str | None, title: str | None, existing_linkedin: str | None) -> dict:
    sources = []
    linkedin_urls = []
    try:
        query = f'"{name}" LinkedIn profile'
        if email:
            query = f'"{email}" "{name}" LinkedIn'
        params = {"q": query}
        async with httpx.AsyncClient(timeout=httpx.Timeout(12)) as c:
            r = await c.get(f"{SEARXNG_URL}/search", params=params)
            r.raise_for_status()
            all_urls = re.findall(r'href="(https?://[^"#]+)"', r.text)
        sources = [u for u in dict.fromkeys(all_urls) if "144.91.126" not in u][:8]
        linkedin_urls = [u for u in sources if "linkedin.com" in u.lower()]
    except Exception as exc:
        log.error("SEARXNG contact search error: %s", exc)

    linkedin_url = existing_linkedin
    if not linkedin_url and linkedin_urls:
        for u in linkedin_urls:
            if "/in/" in u.lower() or "/company/" in u.lower():
                linkedin_url = u
                break
    if not linkedin_url and linkedin_urls:
        linkedin_url = linkedin_urls[0]

    score = 0
    if linkedin_url:
        score += 50
    if title:
        score += 15
    score += min(25, len(sources) * 5)
    if email:
        score += 10

    status = "Enriched Complete" if score >= 80 else "Enriched Partial"

    return {
        "linkedin": linkedin_url,
        "title": title,
        "sources": sources,
        "score": min(score, 100),
        "status": status,
        "notes": f"urls={len(sources)} linkedin={'yes' if linkedin_url else 'no'}",
    }


class EnrichInput(BaseModel):
    company_id: str
    name: str
    website: str | None = None
    country: str | None = None
    industry: str | None = None
    hubspot_id: str | None = None
    company_email: str | None = None
    crm_name: str = "integritasmrv"


class EnrichOutput(BaseModel):
    status: str
    score: int
    notes: str
    sources: list[str]
    hubspot_sync: str


enr_wf = hatchet.workflow(name="enrichment-workflow", input_validator=EnrichInput, on_events=["enrichment-request"])


@enr_wf.task(name="enrich-company")
async def enrich_company(input: EnrichInput, ctx: Context) -> EnrichOutput:
    crm = input.crm_name
    log.info("[%s] Company enrichment started: id=%s name=%s", crm, input.company_id, input.name)

    claimed = await claim_company(input.company_id, crm)
    if not claimed:
        log.warning("[%s] Could not claim company id=%s", crm, input.company_id)
        return EnrichOutput(status="skipped", score=0, notes="already_busy", sources=[], hubspot_sync="skipped")

    enriched = await searxng_enrich_company(claimed["name"], claimed.get("country"), claimed.get("website"))
    enriched["run_id"] = ctx.workflow_run_id

    await write_crm_company(input.company_id, enriched, crm)

    hubspot_sync = await write_hubspot_company(
        claimed.get("hubspot_id") or input.hubspot_id,
        enriched,
        input.company_id,
    )

    log.info(
        "[%s] Company enrichment complete: id=%s status=%s score=%s hs=%s",
        crm, input.company_id, enriched["status"], enriched["score"], hubspot_sync,
    )
    return EnrichOutput(
        status=enriched["status"],
        score=enriched["score"],
        notes=enriched["notes"],
        sources=enriched["sources"],
        hubspot_sync=hubspot_sync,
    )


class ContactEnrichInput(BaseModel):
    contact_id: str
    name: str
    email: str | None = None
    phone: str | None = None
    title: str | None = None
    linkedin: str | None = None
    hubspot_id: str | None = None
    crm_name: str = "integritasmrv"


class ContactEnrichOutput(BaseModel):
    status: str
    score: int
    notes: str
    sources: list[str]
    hubspot_sync: str


contact_wf = hatchet.workflow(
    name="contact-enrichment-workflow",
    input_validator=ContactEnrichInput,
    on_events=["contact-enrichment-request"],
)


@contact_wf.task(name="enrich-contact")
async def enrich_contact(input: ContactEnrichInput, ctx: Context) -> ContactEnrichOutput:
    crm = input.crm_name
    log.info("[%s] Contact enrichment started: id=%s name=%s", crm, input.contact_id, input.name)

    claimed = await claim_contact(input.contact_id, crm)
    if not claimed:
        log.warning("[%s] Could not claim contact id=%s", crm, input.contact_id)
        return ContactEnrichOutput(status="skipped", score=0, notes="already_busy", sources=[], hubspot_sync="skipped")

    enriched = await searxng_enrich_contact(
        claimed["name"],
        claimed.get("email") or input.email,
        claimed.get("title") or input.title,
        claimed.get("linkedin") or input.linkedin,
    )
    enriched["run_id"] = ctx.workflow_run_id

    await write_crm_contact(input.contact_id, enriched, crm)

    hubspot_sync = await write_hubspot_contact(
        claimed.get("hubspot_id") or input.hubspot_id,
        enriched,
        input.contact_id,
    )

    log.info(
        "[%s] Contact enrichment complete: id=%s status=%s score=%s hs=%s",
        crm, input.contact_id, enriched["status"], enriched["score"], hubspot_sync,
    )
    return ContactEnrichOutput(
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


worker = hatchet.worker("integritas-worker", workflows=[cf7_wf, enr_wf, hs_wf, contact_wf])

if __name__ == "__main__":
    log.info("Starting Hatchet worker...")
    worker.start()

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
    "eyJhbGciOiJFUzI1NiIsImtpZCI6ImZ5cTd3QSJ9.eyJhdWQiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCJleHAiOjE3ODQzNjU5MTUsImdycGNfYnJvYWRjYXN0X2FkZHJlc3MiOiJoYXRjaGV0LWVuZ2luZTo3MDcwIiwiaWF0IjoxNzc2NTg5OTE1LCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCJzZXJ2ZXJfdXJsIjoiaHR0cDovL2xvY2FsaG9zdDo4MDgwIiwic3ViIjoiNzA3ZDA4NTUtODBhYi00ZTFmLWExNTYtZjFjNDU0NmNiZjUyIiwidG9rZW5faWQiOiJiMDc5MTc4Zi02OGQ4LTRhMzQtYThlYy1kNzQ2YjE4OGZkMzcifQ.FKSllU33io3P3Dl4bsWvdxPQzHCnXmKpU3PK_vx27DhQvaYjMmFzB91UT2Jw82mvcolXtNBK9tSNP_KcabHCKw",
)
os.environ["HATCHET_CLIENT_TOKEN"] = T
os.environ["HATCHET_CLIENT_HOST_PORT"] = os.environ.get("HATCHET_CLIENT_HOST_PORT", "127.0.0.1:7070")
os.environ["HATCHET_CLIENT_TLS_STRATEGY"] = "none"

hatchet = Hatchet()

SEARXNG_URL = os.environ.get("SEARXNG_URL", "http://144.91.126.111:3010")
CRM_DSN = os.environ.get(
    "CRM_DSN",
    "postgresql://integritasmrv_crm_user:Int3gr1t@smrv_S3cure_P@ssw0rd_2026@10.0.16.2:5432/integritasmrv_crm",
)
HS_TOKEN = os.environ.get("HUBSPOT_API_TOKEN", "")

POWERIQ_HOST = os.environ.get("POWERIQ_DB_HOST", "10.0.20.2")
POWERIQ_PORT = int(os.environ.get("POWERIQ_DB_PORT", "5432"))
POWERIQ_USER = os.environ.get("POWERIQ_DB_USER", "poweriq_crm_user")
POWERIQ_PASS = os.environ.get("POWERIQ_DB_PASS", "P0w3r1Q_CRM_S3cur3_P@ss_2026")
POWERIQ_DB = os.environ.get("POWERIQ_DB_NAME", "poweriq_crm")


async def get_crm_conn():
    return await asyncpg.connect(
        host="crm-integritasmrv-db",
        port=5432,
        user="integritasmrv_crm_user",
        password="oYxxPKRfAHAD263VSDcKmljKY0vInx2QTl6PooKoqmmiDops",
        database="integritasmrv_crm",
        timeout=30,
    )


async def get_poweriq_conn():
    return await asyncpg.connect(
        host=POWERIQ_HOST,
        port=POWERIQ_PORT,
        user=POWERIQ_USER,
        password=POWERIQ_PASS,
        database=POWERIQ_DB,
        timeout=30,
    )


async def insert_cf7_lead_to_poweriq(first_name: str, last_name: str, email: str, phone: str, company: str, message: str) -> int | None:
    conn = await get_poweriq_conn()
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO nb_crm_leads (name, company, email, phone, source, status, description, extra)
            VALUES ($1, $2, $3, $4, 'CF7 Webform', 'new', $5, $6::jsonb)
            RETURNING id
            """,
            f"{first_name} {last_name}".strip(),
            company,
            email,
            phone,
            message,
            json.dumps({"original_message": message}),
        )
        return row["id"] if row else None
    except Exception as e:
        log.error("Failed to insert CF7 lead to PowerIQ: %s", e)
        return None
    finally:
        await conn.close()


def parse_cf7_message(message: str) -> dict:
    result = {
        "first_name": "",
        "last_name": "",
        "email": "",
        "phone": "",
        "company": "",
        "message": message,
    }
    
    name_match = re.search(r'Name:\s*([^(]+)', message)
    if name_match:
        name_parts = name_match.group(1).strip().split()
        if name_parts:
            result["first_name"] = name_parts[0]
            result["last_name"] = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
    
    email_match = re.search(r'Email:\s*([^,]+)', message)
    if email_match:
        result["email"] = email_match.group(1).strip()
    
    phone_match = re.search(r'Phone:\s*([^,]+)', message)
    if phone_match:
        result["phone"] = phone_match.group(1).strip()
    
    company_match = re.search(r'Company:\s*([^,]+)', message)
    if company_match:
        result["company"] = company_match.group(1).strip()
    
    msg_match = re.search(r'Message:\s*(.+)$', message)
    if msg_match:
        result["message"] = msg_match.group(1).strip()
    
    return result


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


async def claim_contact(contact_id: str) -> dict | None:
    conn = await get_crm_conn()
    try:
        row = await conn.fetchrow(
            """
            UPDATE nb_crm_contacts
            SET enrichment_status='Enrichment Busy',
                enrichment_locked_until=NOW() + INTERVAL '30 minutes',
                enrichment_worker_id='hatchet-worker'
            WHERE id = $1::bigint
              AND enrichment_status IN ('To Be Enriched', 'Enrichment Failed')
            RETURNING id, name, email, phone, title, linkedin, hubspot_id, company_id
            """,
            int(contact_id),
        )
        return dict(row) if row else None
    finally:
        await conn.close()


async def write_crm_company(company_id: str, enriched: dict):
    conn = await get_crm_conn()
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
            enriched.get("score", 0),
            enriched.get("notes", ""),
            json.dumps(enriched.get("sources", [])),
            enriched.get("run_id"),
            int(company_id),
        ]
        sql = f"UPDATE nb_crm_customers SET {', '.join(sets)} WHERE id = $17"
        await conn.execute(sql, *values)
        log.info("Wrote enriched company to CRM: id=%s", company_id)
    finally:
        await conn.close()


async def write_crm_contact(contact_id: str, enriched: dict):
    conn = await get_crm_conn()
    try:
        sets = ["email = $1", "phone = $2", "title = $3", "linkedin = $4",
                "industry = $5", "enrichment_status = $6", "enrichment_score = $7",
                "enrichment_notes = $8", "enrichment_source = $9::jsonb",
                "last_enriched_at = NOW()", "enrichment_locked_until = NULL",
                "enrichment_worker_id = NULL", "last_enrichment_run_id = $10"]
        values = [
            enriched.get("email"), enriched.get("phone"), enriched.get("title"),
            enriched.get("linkedin"), enriched.get("industry"),
            enriched.get("status", "Enriched Partial"),
            enriched.get("score", 0),
            enriched.get("notes", ""),
            json.dumps(enriched.get("sources", [])),
            enriched.get("run_id"),
            int(contact_id),
        ]
        sql = f"UPDATE nb_crm_contacts SET {', '.join(sets)} WHERE id = $11"
        await conn.execute(sql, *values)
        log.info("Wrote enriched contact to CRM: id=%s", contact_id)
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


async def searxng_search(query: str) -> list[dict]:
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(
                SEARXNG_URL + "/search",
                params={"q": query, "format": "json", "engines": "google,bing"},
            )
            if r.status_code == 200:
                results = r.json().get("results", [])
                return results[:5]
    except Exception as e:
        log.error("SearXNG search error: %s", e)
    return []


async def searxng_enrich_company(company_name: str, country: str | None = None, website: str | None = None) -> dict:
    query = company_name
    if country:
        query += f" {country}"
    if website:
        query += f" site:{website}"
    
    results = await searxng_search(query)
    
    enriched = {
        "status": "enriched",
        "score": min(100, len(results) * 20),
        "notes": "",
        "sources": [r.get("url", "") for r in results if r.get("url")],
        "website": website or "",
        "linkedin_url": "",
        "description": "",
    }
    
    for r in results:
        url = r.get("url", "")
        title = r.get("title", "").lower()
        if "linkedin" in url:
            enriched["linkedin_url"] = url
        if "description" in r:
            enriched["description"] = r["description"][:500]
    
    enriched["notes"] = f"Found {len(results)} results via SearXNG"
    
    return enriched


class EnrichInput(BaseModel):
    company_id: str
    name: str | None = None
    website: str | None = None
    country: str | None = None
    hubspot_id: str | None = None


class EnrichOutput(BaseModel):
    status: str
    score: int
    notes: str
    sources: list[str]
    hubspot_sync: str


class ContactEnrichInput(BaseModel):
    contact_id: str
    name: str | None = None
    email: str | None = None
    hubspot_id: str | None = None


class ContactEnrichOutput(BaseModel):
    status: str
    score: int
    notes: str
    sources: list[str]
    hubspot_sync: str


class LeadInput(BaseModel):
    message: str


class LeadOutput(BaseModel):
    result: str
    lead_id: int | None = None


cf7_wf = hatchet.workflow(name="cf7-to-crm-workflow", input_validator=LeadInput, on_events=["cf7-lead"])


@cf7_wf.task(name="process-lead")
async def process_lead(input: LeadInput, ctx: Context) -> LeadOutput:
    log.info("CF7 lead: %s", input.message)
    
    parsed = parse_cf7_message(input.message)
    log.info("Parsed CF7 lead: name=%s %s, email=%s, company=%s",
             parsed["first_name"], parsed["last_name"], parsed["email"], parsed["company"])
    
    if not parsed["email"] or "@" not in parsed["email"]:
        log.warning("Invalid email, skipping CRM insert: %s", parsed["email"])
        return LeadOutput(result=f"Skipped: invalid email", lead_id=None)
    
    lead_id = await insert_cf7_lead_to_poweriq(
        first_name=parsed["first_name"],
        last_name=parsed["last_name"],
        email=parsed["email"],
        phone=parsed["phone"],
        company=parsed["company"],
        message=parsed["message"],
    )
    
    if lead_id:
        log.info("Inserted CF7 lead into PowerIQ CRM: id=%d", lead_id)
        return LeadOutput(result=f"Inserted: lead_id={lead_id}", lead_id=lead_id)
    else:
        log.error("Failed to insert CF7 lead into PowerIQ CRM")
        return LeadOutput(result="Failed to insert", lead_id=None)


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


enr_wf = hatchet.workflow(name="enrichment-workflow", input_validator=EnrichInput, on_events=["enrichment-request"])


@enr_wf.task(name="enrich-company")
async def enrich_company(input: EnrichInput, ctx: Context) -> EnrichOutput:
    log.info("Company enrichment started: id=%s name=%s", input.company_id, input.name)

    claimed = await claim_company(input.company_id)
    if not claimed:
        log.warning("Could not claim company id=%s — may already be busy", input.company_id)
        return EnrichOutput(status="skipped", score=0, notes="already_busy", sources=[], hubspot_sync="skipped")

    enriched = await searxng_enrich_company(claimed["name"], claimed.get("country"), claimed.get("website"))
    enriched["run_id"] = ctx.workflow_run_id

    await write_crm_company(input.company_id, enriched)

    hubspot_sync = await write_hubspot_company(
        claimed.get("hubspot_id") or input.hubspot_id,
        enriched,
        input.company_id,
    )

    log.info(
        "Company enrichment complete: id=%s status=%s score=%s hs=%s",
        input.company_id, enriched["status"], enriched["score"], hubspot_sync,
    )
    return EnrichOutput(
        status=enriched["status"],
        score=enriched["score"],
        notes=enriched["notes"],
        sources=enriched["sources"],
        hubspot_sync=hubspot_sync,
    )


contact_wf = hatchet.workflow(
    name="contact-enrichment-workflow",
    input_validator=ContactEnrichInput,
    on_events=["contact-enrichment-request"],
)


@contact_wf.task(name="enrich-contact")
async def enrich_contact(input: ContactEnrichInput, ctx: Context) -> ContactEnrichOutput:
    log.info("Contact enrichment started: id=%s name=%s", input.contact_id, input.name)

    claimed = await claim_contact(input.contact_id)
    if not claimed:
        log.warning("Could not claim contact id=%s — may already be busy", input.contact_id)
        return ContactEnrichOutput(status="skipped", score=0, notes="already_busy", sources=[], hubspot_sync="skipped")

    query = f"{claimed.get('name')} {claimed.get('email')}"
    if claimed.get("company"):
        query += f" {claimed['company']}"
    results = await searxng_search(query)
    
    enriched = {
        "status": "enriched",
        "score": min(100, len(results) * 20),
        "notes": f"Found {len(results)} results",
        "sources": [r.get("url", "") for r in results if r.get("url")],
        "email": claimed.get("email"),
        "phone": claimed.get("phone"),
        "title": claimed.get("title"),
        "linkedin": claimed.get("linkedin"),
        "run_id": ctx.workflow_run_id,
    }

    await write_crm_contact(input.contact_id, enriched)

    hubspot_sync = await write_hubspot_contact(
        claimed.get("hubspot_id") or input.hubspot_id,
        enriched,
        input.contact_id,
    )

    log.info(
        "Contact enrichment complete: id=%s status=%s score=%s hs=%s",
        input.contact_id, enriched["status"], enriched["score"], hubspot_sync,
    )
    return ContactEnrichOutput(
        status=enriched["status"],
        score=enriched["score"],
        notes=enriched["notes"],
        sources=enriched["sources"],
        hubspot_sync=hubspot_sync,
    )


worker = hatchet.worker("integritas-worker", workflows=[cf7_wf, enr_wf, hs_wf, contact_wf])

if __name__ == "__main__":
    log.info("Starting Hatchet worker...")
    worker.start()

import httpx
import os
from temporalio import activity


HUBSPOT_BASE = "https://api.hubapi.com"
PAT = os.environ.get("HUBSPOT_PAT", "")


@activity.defn
async def trigger_enrichiq(entity_type: str, business_key: str, round_num: int, source: str) -> dict:
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://enrichiq.integritasmrv.com/api/trigger",
            json={
                "entity_type": entity_type,
                "business_key": business_key,
                "round": round_num,
                "source": source,
            }
        )
        return {"status_code": resp.status_code, "body": resp.text}


@activity.defn
def update_hubspot_contact(hubspot_id: str, enriched_data: dict) -> dict:
    properties = {}
    ea = enriched_data.get("entity_attributes", {})
    if enriched_data.get("enrichment_score") is not None:
        properties["enrichment_score"] = str(enriched_data["enrichment_score"])
    if enriched_data.get("last_enriched_at"):
        properties["last_enriched_at"] = enriched_data["last_enriched_at"]
    if ea.get("phone"):
        properties["phone"] = ea["phone"]
    if ea.get("linkedin_url"):
        properties["linkedin_url"] = ea["linkedin_url"]
    if ea.get("job_title"):
        properties["job_title"] = ea["job_title"]
    if ea.get("city"):
        properties["city"] = ea["city"]
    if ea.get("country"):
        properties["country"] = ea["country"]

    if not properties:
        return {"updated": False, "reason": "no_properties_to_sync"}

    with httpx.Client() as client:
        resp = client.patch(
            f"{HUBSPOT_BASE}/crm/v3/objects/contacts/{hubspot_id}",
            headers={
                "Authorization": f"Bearer {PAT}",
                "Content-Type": "application/json",
            },
            json={"properties": properties},
        )

    if resp.status_code == 200:
        return {"updated": True, "contact_id": hubspot_id}
    else:
        return {"updated": False, "status_code": resp.status_code, "body": resp.text}


@activity.defn
def update_hubspot_company(hubspot_id: str, enriched_data: dict) -> dict:
    properties = {}
    ea = enriched_data.get("entity_attributes", {})
    if enriched_data.get("enrichment_score") is not None:
        properties["enrichment_score"] = str(enriched_data["enrichment_score"])
    if enriched_data.get("last_enriched_at"):
        properties["last_enriched_at"] = enriched_data["last_enriched_at"]
    for field in ["phone", "website", "linkedin_url", "city", "country", "industry", "description"]:
        if ea.get(field):
            properties[field] = ea[field]

    if not properties:
        return {"updated": False, "reason": "no_properties_to_sync"}

    with httpx.Client() as client:
        resp = client.patch(
            f"{HUBSPOT_BASE}/crm/v3/objects/companies/{hubspot_id}",
            headers={
                "Authorization": f"Bearer {PAT}",
                "Content-Type": "application/json",
            },
            json={"properties": properties},
        )

    if resp.status_code == 200:
        return {"updated": True, "company_id": hubspot_id}
    else:
        return {"updated": False, "status_code": resp.status_code, "body": resp.text}

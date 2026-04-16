from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import Optional
import asyncio
import os
import time
import asyncpg
import httpx
from temporalio.client import Client


app = FastAPI(title="IntegritasMRV Temporal Bridge")

TEMPORAL_ADDR = "10.0.4.16:7233"
TASK_QUEUE = "integritasmrv-ingest"

_db_pool = None
_temporal_client = None


@app.on_event("startup")
async def startup():
    global _db_pool, _temporal_client
    _db_pool = await asyncpg.create_pool(
        host="10.0.13.2", port=5432, database="integritasmrv_crm",
        user="integritasmrv_crm_user", password="Int3gr1t@smrv_S3cure_P@ssw0rd_2026",
        min_size=1, max_size=5,
    )
    _temporal_client = await Client.connect(TEMPORAL_ADDR)


class WritebackPayload(BaseModel):
    entity_id: int
    target_crm: str
    source_system: str
    entity_type: str = "contact"
    enriched_data: dict
    external_ids: dict


class EnrichIQWriteback(BaseModel):
    entity: dict
    trusted_attributes: dict
    meta: dict


class WebformPayload(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    company: Optional[str] = None
    company_website: Optional[str] = None
    function: Optional[str] = None
    product_interest: Optional[str] = None
    message: Optional[str] = None
    source_url: Optional[str] = None
    form_id: Optional[str] = None
    form_title: Optional[str] = None


@app.post("/webhook/hubspot")
async def webhook_hubspot(request: Request):
    try:
        body = await request.json()
        print(f"[WEBHOOK] raw body: {str(body)[:800]}", flush=True)

        items = []
        if isinstance(body, list):
            items = body
        elif isinstance(body, dict):
            if "objects" in body:
                items = body["objects"]
            elif "objectId" in body or "properties" in body:
                items = [body]
            elif "data" in body:
                inner = body["data"]
                items = [{"properties": inner.get("properties", inner)}] if isinstance(inner, dict) else [body]
            elif "subscriptionType" in body:
                items = [body]
            else:
                items = [body]

        hubspot_pat = os.environ.get("HUBSPOT_PAT", "")
        results = []

        async with _db_pool.acquire() as conn:
            for item in items:
                sub_type = item.get("subscriptionType", "")
                object_id = str(item.get("objectId", ""))
                is_hubspot_event = bool(sub_type)

                if "company" in sub_type:
                    object_type = "company"
                elif "contact" in sub_type:
                    object_type = "contact"
                elif "deal" in sub_type:
                    object_type = "deal"
                else:
                    object_type = item.get("objectType", "contact").lower()

                props = item.get("properties", {})
                property_name = item.get("propertyName", "")
                is_owner_change = (property_name == "hubspot_owner_id")
                owner_id_from_event = item.get("propertyValue", "")

                if is_hubspot_event and object_id:
                    print(f"[WEBHOOK] HubSpot event: {sub_type} id={object_id} prop={property_name}", flush=True)
                    async with httpx.AsyncClient(timeout=15.0) as hc:
                        if object_type == "company":
                            url = f"https://api.hubapi.com/crm/v3/objects/companies/{object_id}?properties=name,phone,website,industry,description,country,city,hubspot_owner_id,hubspot_company_id,hs_object_id,vat_number__c,linkedin_company_page"
                        else:
                            url = f"https://api.hubapi.com/crm/v3/objects/contacts/{object_id}?properties=firstname,lastname,email,phone,jobtitle,city,country,website,linkedin_primary_company,hubspot_owner_id,hs_object_id,company"
                        try:
                            resp = await hc.get(url, headers={
                                "Authorization": f"Bearer {hubspot_pat}",
                                "Content-Type": "application/json",
                            })
                            if resp.status_code == 200:
                                data = resp.json()
                                fetched_props = data.get("properties", {})
                                fetched_owner = fetched_props.get("hubspot_owner_id", "")
                                if is_owner_change:
                                    props = fetched_props
                                    owner_id = owner_id_from_event
                                    print(f"[WEBHOOK] Owner change: API owner={fetched_owner}, routing by event owner={owner_id}", flush=True)
                                else:
                                    props = fetched_props
                                    owner_id = fetched_owner
                                    print(f"[WEBHOOK] Fetched {object_type} {object_id}: owner={owner_id}", flush=True)
                            else:
                                print(f"[WEBHOOK] HubSpot API {resp.status_code}: {resp.text[:200]}", flush=True)
                                owner_id = owner_id_from_event or ""
                        except Exception as e:
                            print(f"[WEBHOOK] HubSpot API error: {e}", flush=True)
                            owner_id = owner_id_from_event or ""
                else:
                    owner_id = props.get("hubspot_owner_id", "")

                print(f"[WEBHOOK] type={object_type} id={object_id} owner={owner_id}", flush=True)

                route_row = await conn.fetchrow(
                    "SELECT target_crm FROM crm_sync_routing WHERE hubspot_owner_id = $1 AND active = true LIMIT 1",
                    str(owner_id),
                )

                if route_row:
                    target_crm = route_row["target_crm"].lower()
                else:
                    target_crm = "integritasmrv"

                business_key = str(object_id) if object_id else None

                await _temporal_client.start_workflow(
                    "IngestWorkflow",
                    {
                        "source": "hubspot",
                        "mapping_name": "hubspot_to_crm",
                        "target_crm": target_crm,
                        "business_key": business_key,
                        "data": {"properties": props, "objectType": object_type, "objectId": object_id},
                    },
                    id=f"ingest-hubspot-{business_key}",
                    task_queue=TASK_QUEUE,
                )

                results.append({"objectId": object_id, "status": "accepted", "target_crm": target_crm})

        return {"status": "ok", "processed": len(results), "results": results}
    except Exception as e:
        print(f"[WEBHOOK] Error: {e}", flush=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ingest/webform")
async def ingest_webform(request: Request):
    try:
        payload = await request.json()
        email = payload.get("your-email") or payload.get("email", "unknown")
        first_name = payload.get("first-name", "")
        last_name = payload.get("last-name", "")
        wf_id = f"webform-{email}-{int(time.time())}"
        client = await Client.connect(TEMPORAL_ADDR)
        await client.start_workflow(
            "IngestWorkflow",
            {
                "source": "webform",
                "mapping_name": "webform_to_crm",
                "target_crm": "poweriq",
                "business_key": f"{email}-{int(time.time())}",
                "data": {
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email,
                    "phone": payload.get("phone", ""),
                    "company": payload.get("company", ""),
                    "company_website": payload.get("company-website", ""),
                    "job_title": payload.get("function", ""),
                    "product_interest": payload.get("product-interest", ""),
                    "message": payload.get("message", ""),
                    "source_url": payload.get("source_url", ""),
                    "form_id": payload.get("form_id", ""),
                    "form_title": payload.get("form_title", ""),
                },
            },
            id=wf_id,
            task_queue=TASK_QUEUE,
        )
        return {"status": "accepted", "workflow_id": wf_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/writeback")
async def writeback(payload: WritebackPayload, request: Request):
    try:
        wf_id = f"writeback-{payload.entity_id}"
        client = await Client.connect(TEMPORAL_ADDR)
        await client.start_workflow(
            "WritebackWorkflow",
            {
                "entity_id": payload.entity_id,
                "target_crm": payload.target_crm,
                "source_system": payload.source_system,
                "entity_type": payload.entity_type,
                "enriched_data": payload.enriched_data,
                "external_ids": payload.external_ids,
            },
            id=wf_id,
            task_queue=TASK_QUEUE,
        )
        return {"status": "accepted", "workflow_id": wf_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/enrichiq/writeback")
async def enrichiq_writeback(payload: EnrichIQWriteback, request: Request):
    try:
        meta = payload.meta or {}
        entity_id = meta.get("entity_id")
        target_crm = meta.get("target_crm", "integritasmrv")
        source_system = meta.get("source_system", "hubspot")
        entity_type = meta.get("entity_type", "contact")
        external_ids = meta.get("external_ids", {})

        if not entity_id:
            raise HTTPException(status_code=400, detail="entity_id required in meta")

        enriched_data = {
            "entity_attributes": payload.entity or {},
            "enrichment_score": payload.trusted_attributes.get("enrichment_score"),
            "last_enriched_at": payload.trusted_attributes.get("last_enriched_at"),
        }

        wf_id = f"writeback-enrichiq-{entity_id}"
        client = await Client.connect(TEMPORAL_ADDR)
        await client.start_workflow(
            "WritebackWorkflow",
            {
                "entity_id": entity_id,
                "target_crm": target_crm,
                "source_system": source_system,
                "entity_type": entity_type,
                "enriched_data": enriched_data,
                "external_ids": external_ids,
            },
            id=wf_id,
            task_queue=TASK_QUEUE,
        )
        return {"status": "accepted", "workflow_id": wf_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/chatwoot/webhook")
async def chatwoot_webhook(request: Request):
    try:
        payload = await request.json()
        if payload.get("event") != "message_created":
            return {"handled": False, "reason": "not message_created"}
        message = payload.get("message", {})
        if message.get("message_type") != "incoming":
            return {"handled": False, "reason": "not incoming"}
        user_message = message.get("content", "")
        conversation_id = payload.get("conversation_id")
        account_id = payload.get("account_id")
        if not user_message or not conversation_id:
            return {"handled": False, "reason": "missing data"}
        rag_context = ""
        try:
            with httpx.Client(timeout=15.0) as client:
                rag_resp = client.post(
                    "http://intelligence-lightrag:9621/query/data",
                    json={"query": user_message, "mode": "hybrid"},
                    headers={"LIGHTRAG-WORKSPACE": "poweriq"}
                )
                rag_data = rag_resp.json()
                chunks = rag_data.get("data", {}).get("chunks", [])
                texts = [c.get("content", "")[:500] for c in chunks[:2]]
                rag_context = "\n\n".join(texts)
        except Exception as e:
            print(f"RAG error: {e}")
        system_prompt = f"""You are a helpful sales assistant. If the user wants to speak with a human, respond with exactly: [TRANSFER]
Context from knowledge base:
{rag_context if rag_context else 'No specific context available.'}"""
        try:
            with httpx.Client(timeout=45.0) as client:
                llm_resp = client.post(
                    "http://10.0.4.19:4000/v1/chat/completions",
                    headers={
                        "Authorization": "Bearer sk-litellm-aifabric-secret",
                        "Content-Type": "application/json"
                    },
                json={
                    "model": "fast-local",
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_message}
                        ],
                        "max_tokens": 300,
                        "temperature": 0.7
                    }
                )
                llm_data = llm_resp.json()
                if llm_data.get("error"):
                    raise Exception(llm_data["error"].get("message", "LLM error"))
                ai_response = llm_data.get("choices", [{}])[0].get("message", {}).get("content", "I'm having trouble responding right now.")
                should_handoff = "[TRANSFER]" in ai_response
                ai_response = ai_response.replace("[TRANSFER]", "").strip()
        except Exception as e:
            print(f"LLM error: {e}")
            ai_response = "I'm having trouble responding right now. A human agent will be with you shortly."
            should_handoff = True
        try:
            with httpx.Client(timeout=10.0) as client:
                login_resp = client.post(
                    "https://chat.belinus.net/auth/sign_in",
                    json={"email": "admin@belinus.net", "password": "BelinusAdmin123!"}
                )
                login_data = login_resp.json()
                token = login_data.get("data", {}).get("access_token")
                if token:
                    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                    if should_handoff:
                        client.patch(
                            f"https://chat.belinus.net/api/v1/accounts/{account_id}/conversations/{conversation_id}",
                            headers=headers,
                            json={"status": "open"}
                        )
                    client.post(
                        f"https://chat.belinus.net/api/v1/accounts/{account_id}/conversations/{conversation_id}/messages",
                        headers=headers,
                        json={"content": ai_response, "message_type": "outgoing"}
                    )
        except Exception as e:
            print(f"Chatwoot post error: {e}")
        return {"handled": True, "reply": ai_response, "handoff": should_handoff}
    except Exception as e:
        print(f"Webhook error: {e}")
        return {"handled": False, "error": str(e)}


@app.get("/health")
async def health():
    return {"status": "ok", "temporal": TEMPORAL_ADDR, "namespace": "Integritasmrv"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=15579)

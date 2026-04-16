from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import Optional
import asyncio
import time
import httpx
from temporalio.client import Client


app = FastAPI(title="IntegritasMRV Temporal Bridge")

TEMPORAL_ADDR = "10.0.4.16:7233"
TASK_QUEUE = "integritasmrv-ingest"


class HubspotWebhookPayload(BaseModel):
    source: str = "hubspot"
    data: dict
    target_crm: Optional[str] = None


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
async def webhook_hubspot(payload: HubspotWebhookPayload, request: Request):
    try:
        routing_rules = payload.data.get("routing_rules", {})
        target_crm = payload.target_crm or routing_rules.get("target_crm", "integritasmrv")

        business_key = (
            payload.data.get("properties", {})
            .get("hs_object_id")
        )
        wf_id = f"ingest-hubspot-{business_key}"

        client = await Client.connect(TEMPORAL_ADDR)
        await client.start_workflow(
            "IngestWorkflow",
            {
                "source": payload.source,
                "mapping_name": "hubspot_to_crm",
                "target_crm": target_crm,
                "business_key": str(business_key) if business_key else None,
                "data": payload.data,
            },
            id=wf_id,
            task_queue=TASK_QUEUE,
        )

        return {"status": "accepted", "workflow_id": wf_id, "target_crm": target_crm}
    except Exception as e:
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


async def query_rag(query: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            rag_resp = await client.post(
                "http://intelligence-lightrag:9621/query/data",
                json={"query": query, "mode": "hybrid"},
                headers={"LIGHTRAG-WORKSPACE": "poweriq"}
            )
            if rag_resp.status_code == 200:
                rag_data = rag_resp.json()
                chunks = rag_data.get("data", {}).get("chunks", [])
                texts = [c.get("content", "")[:250] for c in chunks[:2]]
                return "\n\n".join(texts)
    except Exception as e:
        print(f"RAG error: {e}")
    return ""


async def post_typing(account_id: int, conversation_id: int, typing: bool):
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(
                f"https://chat.belinus.net/api/v1/accounts/{account_id}/conversations/{conversation_id}/toggle_typing",
                headers={"api_access_token": "3JGewDYGGD78t7s1zB4rbskk"},
                json={"is_typing": typing}
            )
    except Exception as e:
        print(f"Typing indicator error: {e}")


async def post_reply(account_id: int, conversation_id: int, message: str):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            f"https://chat.belinus.net/api/v1/accounts/{account_id}/conversations/{conversation_id}/messages",
            headers={"api_access_token": "3JGewDYGGD78t7s1zB4rbskk", "Content-Type": "application/json"},
            json={"content": message, "message_type": "outgoing"}
        )
        return resp


@app.get("/chatwoot/webhook")
async def chatwoot_webhook_get():
    return {"status": "ok"}


@app.post("/chatwoot/webhook")
async def chatwoot_webhook(request: Request):
    try:
        payload = await request.json()
        
        if payload.get("event") != "message_created":
            return {"handled": False, "reason": "not message_created"}
        
        conversation_id = payload.get("conversation", {}).get("id")
        account_id = payload.get("account_id") or 1
        message_data = payload.get("message", {})
        msg_type = message_data.get("message_type") if message_data else None
        
        if not msg_type:
            messages = payload.get("conversation", {}).get("messages", [])
            if messages:
                msg_type = messages[0].get("message_type")
                message_data = messages[0]
        
        if str(msg_type) not in ("0", "incoming", "1"):
            return {"handled": False, "reason": f"not incoming, got {msg_type}"}
        
        user_message = message_data.get("content", "") if message_data else payload.get("content", "")
        
        if not user_message or not conversation_id:
            return {"handled": False, "reason": f"missing data"}
        
        # Post typing indicator immediately
        await post_typing(account_id, conversation_id, True)
        
        # Fetch RAG in parallel (saves ~0.5-1s)
        rag_context = await query_rag(user_message)
        
        system_prompt = f"""You are a helpful sales assistant for Belinus, a Belgian company specializing in battery storage and energy solutions. IMPORTANT: You ONLY discuss Belinus products and services. NEVER mention other companies or products.

RESPOND IN THE SAME LANGUAGE as the user's message (English, Dutch, or French).

Products:
- Energywall G1: Lithium-free residential energy storage using graphene supercapacitor technology
- 50000 charge cycles, 99% round-trip efficiency
- 10 year warranty, 25 year design life
- Modular from 5kWh to 500kWh

About Belinus:
- Belgian engineering company
- Headquarters at Thor Park Genk, Belgium
- Website: www.belinus.net
- Founded 2015, acquired by RBD N.V. in 2024

If the user wants to speak with a human, respond with exactly: [TRANSFER]

Use Belinus information above AND context from knowledge base below.

Context:
{rag_context if rag_context else 'No context available.'}"""
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                llm_resp = await client.post(
                    "http://10.0.4.19:4000/v1/chat/completions",
                    headers={
                        "Authorization": "Bearer sk-litellm-aifabric-secret",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": "minimax-m2.7",
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_message}
                        ],
                        "max_tokens": 200,
                        "temperature": 0.1
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
        
        # Stop typing indicator
        await post_typing(account_id, conversation_id, False)
        
        try:
            # Use the admin API token directly
            headers = {"api_access_token": "3JGewDYGGD78t7s1zB4rbskk", "Content-Type": "application/json"}
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                if should_handoff:
                    await client.patch(
                        f"https://chat.belinus.net/api/v1/accounts/{account_id}/conversations/{conversation_id}",
                        headers=headers,
                        json={"status": "open"}
                    )
                
                await client.post(
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

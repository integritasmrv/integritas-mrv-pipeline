from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import Optional, AsyncIterator
import asyncio
import time
import re
import hashlib
import json
import httpx
from temporalio.client import Client


app = FastAPI(title="IntegritasMRV Temporal Bridge")

TEMPORAL_ADDR = "10.0.4.16:7233"
TASK_QUEUE = "integritasmrv-ingest"

REDIS_HOST = "10.0.4.27"
REDIS_PORT = 6379
CACHE_TTL = 3600

DIRECT_PATTERNS = [
    r"^(hi|hello|hey|bonjour|salut|hallo|goededag|goeie|bonsoir)",
    r"^(merci|thanks|thank you|dank u|danke|bedankt)$",
    r"^(ok|okay|yes|no|oui|non|ja)$",
    r"^(goodbye|bye|tot ziens|à bientôt|adieu)$",
    r"^(how are you|how do you do|ça va)$",
    r"^what is your name$",
    r"^(?i)(who are you|wat ben jij)$",
]

def needs_retrieval(query: str) -> bool:
    q = query.strip().lower()
    for pattern in DIRECT_PATTERNS:
        if re.match(pattern, q):
            return False
    return True

def cache_key(query: str, prefix: str = "rag") -> str:
    return f"{prefix}:{hashlib.sha256(query.encode()).hexdigest()}"

async def get_cached(key: str) -> Optional[str]:
    try:
        import redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=1, socket_timeout=1)
        cached = r.get(key)
        if cached:
            return cached.decode()
    except Exception as e:
        print(f"Cache get error: {e}")
    return None

async def set_cached(key: str, value: str):
    try:
        import redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=1, socket_timeout=1)
        r.setex(key, CACHE_TTL, value)
    except Exception as e:
        print(f"Cache set error: {e}")

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

class AskRequest(BaseModel):
    query: str
    stream: bool = True

@app.post("/webhook/hubspot")
async def webhook_hubspot(payload: HubspotWebhookPayload, request: Request):
    try:
        routing_rules = payload.data.get("routing_rules", {})
        target_crm = payload.target_crm or routing_rules.get("target_crm", "integritasmrv")
        business_key = payload.data.get("properties", {}).get("hs_object_id")
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
        
        await post_typing(account_id, conversation_id, True)
        
        if not needs_retrieval(user_message):
            rag_context = ""
            print(f"QueryRouter: bypass RAG for: {user_message[:30]}")
        else:
            ck = cache_key(user_message, "rag")
            cached = await get_cached(ck)
            if cached:
                rag_context = cached
                print(f"Cache hit for: {user_message[:30]}")
            else:
                rag_context = await query_rag(user_message)
                await set_cached(ck, rag_context)
        
        system_prompt = f"""Je bent een behulpzame verkoopassistent voor Belinus, een Belgisch bedrijf gespecialiseerd in batterijopslag en energieoplossingen. IMPORTANT: Je bespreekt ALLEEN Belinus producten en diensten.

RESPONDEER IN DEZELFDE TAAL als de gebruiker (Nederlands, Engels of Frans).

Producten:
- Energywall G1: Lithium-vrije residentiële energieopslag met graf supercapacitor technologie
- 50000 laadcycli, 99% round-trip efficiëntie
- 10 jaar garantie, 25 jaar ontwerplevensduur
- Modulair van 5kWh tot 500kWh

Over Belinus:
- Belgisch ingenieursbureau
- Hoofdkantoor op Thor Park Genk, België
- Website: www.belinus.net
- Opgericht in 2015, overgenomen door RBD N.V. in 2024

Als de gebruiker een mens wil spreken, antwoord dan exact: [TRANSFER]

Context van kennisbank:
{rag_context if rag_context else 'Geen specifieke context beschikbaar.'}"""
        
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
                        "max_tokens": 150,
                        "temperature": 0.1
                    }
                )
                llm_data = llm_resp.json()
                if llm_data.get("error"):
                    raise Exception(str(llm_data["error"]))
                
                ai_response = llm_data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
                if not ai_response:
                    ai_response = "Ik heb even geen antwoord. Een mens zal zo snel mogelijk reageren."
                    should_handoff = True
                else:
                    should_handoff = "[TRANSFER]" in ai_response
                    ai_response = ai_response.replace("[TRANSFER]", "").strip()
        except Exception as e:
            import traceback
            print(f"LLM error: {type(e).__name__}: {e}")
            ai_response = "Ik heb even geen antwoord. Een mens zal zo snel mogelijk reageren."
            should_handoff = True
        
        await post_typing(account_id, conversation_id, False)
        
        try:
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
                print(f"Response sent: {ai_response[:50]}")
        except Exception as e:
            print(f"Chatwoot post error: {e}")
        
        return {"handled": True, "reply": ai_response, "handoff": should_handoff}
        
    except Exception as e:
        print(f"Webhook error: {e}")
        return {"handled": False, "error": str(e)}

@app.get("/health")
async def health():
    return {"status": "ok", "temporal": TEMPORAL_ADDR, "namespace": "Integritasmrv"}

@app.post("/ask")
async def ask(body: AskRequest):
    query = body.query
    
    if not needs_retrieval(query):
        rag_context = ""
        print(f"QueryRouter: bypass RAG for: {query[:30]}")
    else:
        ck = cache_key(query, "rag")
        cached = await get_cached(ck)
        if cached:
            rag_context = cached
            print(f"Cache hit for: {query[:30]}")
        else:
            rag_context = await query_rag(query)
            await set_cached(ck, rag_context)
    
    context_text = f"Context:\n{rag_context}\n\n" if rag_context else ""
    prompt = f"""{context_text}Question: {query}

Answer concisely in the same language as the question (EN/NL/FR)."""

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                "http://10.0.4.19:4000/v1/chat/completions",
                headers={
                    "Authorization": "Bearer sk-litellm-aifabric-secret",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "minimax-m2.7",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 150,
                    "temperature": 0.1
                }
            )
            data = resp.json()
            answer = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return {"answer": answer, "context_used": bool(rag_context)}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=15579)

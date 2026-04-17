from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import Optional
import asyncio
import time
import re
import hashlib
import httpx
from temporalio.client import Client

app = FastAPI(title="IntegritasMRV Chat + RAG")

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
]

def needs_retrieval(query: str) -> bool:
    q = query.strip().lower()
    for pattern in DIRECT_PATTERNS:
        if re.match(pattern, q):
            return False
    return True

def cache_key_fn(query: str, prefix: str = "rag") -> str:
    return f"{prefix}:{hashlib.sha256(query.encode()).hexdigest()}"

async def get_cached(key: str) -> Optional[str]:
    try:
        import redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=1, socket_timeout=1)
        cached = r.get(key)
        return cached.decode() if cached else None
    except:
        pass
    return None

async def set_cached(key: str, value: str):
    try:
        import redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=1, socket_timeout=1)
        r.setex(key, CACHE_TTL, value)
    except:
        pass

def extract_llm_response(data: dict) -> str:
    msg = data.get("choices", [{}])[0].get("message", {})
    ai_response = msg.get("content", "").strip()
    if not ai_response:
        ai_response = msg.get("reasoning_content", "").strip()
    if not ai_response:
        ai_response = msg.get("provider_specific_fields", {}).get("reasoning_content", "").strip()
    return ai_response

class HubspotPayload(BaseModel):
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

class EnrichIQPayload(BaseModel):
    entity: dict
    trusted_attributes: dict
    meta: dict

class AskRequest(BaseModel):
    query: str
    stream: bool = True

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/webhook/hubspot")
async def webhook_hubspot(request: Request):
    try:
        body = await request.json()
        print(f"[WEBHOOK] HubSpot event received", flush=True)
        
        client = await Client.connect(TEMPORAL_ADDR)
        business_key = body.get("objectId") or body.get("properties", {}).get("hs_object_id", "unknown")
        
        await client.start_workflow(
            "IngestWorkflow",
            {
                "source": "hubspot",
                "mapping_name": "hubspot_to_crm",
                "target_crm": "integritasmrv",
                "business_key": str(business_key),
                "data": body,
            },
            id=f"ingest-hubspot-{business_key}",
            task_queue=TASK_QUEUE,
        )
        return {"status": "accepted", "workflow_id": f"ingest-hubspot-{business_key}"}
    except Exception as e:
        print(f"HubSpot webhook error: {e}")
        return {"status": "error", "detail": str(e)}

@app.post("/ingest/webform")
async def ingest_webform(request: Request):
    try:
        body = await request.json()
        email = body.get("your-email") or body.get("email", "unknown")
        client = await Client.connect(TEMPORAL_ADDR)
        wf_id = f"webform-{email}-{int(time.time())}"
        
        await client.start_workflow(
            "IngestWorkflow",
            {
                "source": "webform",
                "mapping_name": "webform_to_crm",
                "target_crm": "poweriq",
                "business_key": f"{email}-{int(time.time())}",
                "data": {
                    "first_name": body.get("first-name", ""),
                    "last_name": body.get("last-name", ""),
                    "email": email,
                    "phone": body.get("phone", ""),
                    "company": body.get("company", ""),
                    "company_website": body.get("company-website", ""),
                    "job_title": body.get("function", ""),
                    "message": body.get("message", ""),
                },
            },
            id=wf_id,
            task_queue=TASK_QUEUE,
        )
        return {"status": "accepted", "workflow_id": wf_id}
    except Exception as e:
        print(f"Webform error: {e}")
        return {"status": "error", "detail": str(e)}

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
                return "\n\n".join([c.get("content", "")[:300] for c in chunks[:3]])
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
    except:
        pass

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
            return {"handled": False}
        
        user_message = message_data.get("content", "") if message_data else ""
        
        if not user_message or not conversation_id:
            return {"handled": False}
        
        await post_typing(account_id, conversation_id, True)
        
        if not needs_retrieval(user_message):
            rag_context = ""
            print(f"Router: bypass for '{user_message[:30]}'")
        else:
            ck = cache_key_fn(user_message)
            cached = await get_cached(ck)
            if cached:
                rag_context = cached
                print(f"Cache hit for '{user_message[:30]}'")
            else:
                rag_context = await query_rag(user_message)
                await set_cached(ck, rag_context)
        
        prompt = f"""Je bent een behulpzame en vriendelijke verkoopassistent voor Belinus, een Belgisch bedrijf dat lithium-vrije batterijopslagsystemen maakt.

Producten:
- Energywall G1: Lithium-vrije thuisbatterij met grafeen supercapacitor technologie
- 50000 laadcycli, 99% efficiëntie, 10 jaar garantie
- Modulair: 5kWh tot 500kWh

Belangrijk: Beantwoord KORT en VRIENDELIJK in dezelfde taal als de gebruiker (NL/EN/FR).
Als iemand een mens wil spreken, schrijf dan: [TRANSFER]

Vraag: {user_message}
Antwoord:"""

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
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 150,
                        "temperature": 0.1
                    }
                )
                data = llm_resp.json()
                if data.get("error"):
                    raise Exception(str(data["error"]))
                
                ai_response = extract_llm_response(data)
                if not ai_response:
                    ai_response = "Bedankt voor je bericht! Een van onze teamleden neemt snel contact op."
                    handoff = True
                else:
                    handoff = "[TRANSFER]" in ai_response
                    ai_response = ai_response.replace("[TRANSFER]", "").strip()
        except Exception as e:
            print(f"LLM error: {e}")
            ai_response = "Bedankt voor je bericht! Een van onze teamleden neemt snel contact op."
            handoff = True
        
        await post_typing(account_id, conversation_id, False)
        
        try:
            hdrs = {"api_access_token": "3JGewDYGGD78t7s1zB4rbskk", "Content-Type": "application/json"}
            async with httpx.AsyncClient(timeout=10.0) as client:
                if handoff:
                    await client.patch(
                        f"https://chat.belinus.net/api/v1/accounts/{account_id}/conversations/{conversation_id}",
                        headers=hdrs,
                        json={"status": "open"}
                    )
                await client.post(
                    f"https://chat.belinus.net/api/v1/accounts/{account_id}/conversations/{conversation_id}/messages",
                    headers=hdrs,
                    json={"content": ai_response, "message_type": "outgoing"}
                )
                print(f"Sent: {ai_response[:50]}")
        except Exception as e:
            print(f"Chatwoot post error: {e}")
        
        return {"handled": True}
    except Exception as e:
        print(f"Webhook error: {e}")
        return {"handled": False}

@app.post("/ask")
async def ask(body: AskRequest):
    query = body.query
    
    if not needs_retrieval(query):
        ctx = ""
        print(f"Router: bypass for '{query[:30]}'")
    else:
        ck = cache_key_fn(query)
        cached = await get_cached(ck)
        if cached:
            ctx = cached
            print(f"Cache hit for '{query[:30]}'")
        else:
            ctx = await query_rag(query)
            await set_cached(ck, ctx)
    
    prompt = f"""Context: {ctx if ctx else 'Geen specifieke context'}
Vraag: {query}
Antwoord kort en behulpzaam:"""

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
            ai_response = extract_llm_response(data)
            return {"answer": ai_response, "context_used": bool(ctx), "cached": False}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=15579)
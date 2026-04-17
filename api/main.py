from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import Optional, Tuple
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

MODELS = ["minimax-m2.7", "fast-local", "qwen2.5-14b"]
MAX_RETRIES = 1
LLM_TIMEOUT = 15.0
OLLAMA_DIRECT = "http://10.0.4.27:11434"
OLLAMA_DIRECT_MODEL = "llama3.2:latest"
OLLAMA_DIRECT_TIMEOUT = 10.0

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

def extract_llm_response(data: dict) -> Optional[str]:
    try:
        choices = data.get("choices", [{}])
        if not choices:
            return None
        msg = choices[0].get("message", {})
        content = msg.get("content")
        if content and content.strip():
            return content.strip()
        rc = msg.get("reasoning_content")
        if rc and rc.strip():
            return rc.strip()
        psf = msg.get("provider_specific_fields", {})
        if isinstance(psf, dict):
            rc2 = psf.get("reasoning_content")
            if rc2 and rc2.strip():
                return rc2.strip()
        return None
    except Exception as e:
        print(f"extract_llm_response error: {e}")
        return None

async def call_llm(prompt: str, model: str) -> Tuple[Optional[str], bool]:
    try:
        async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as client:
            resp = await client.post(
                "http://10.0.4.19:4000/v1/chat/completions",
                headers={
                    "Authorization": "Bearer sk-litellm-aifabric-secret",
                    "Content-Type": "application/json"
                },
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 150,
                    "temperature": 0.1
                }
            )
            data = resp.json()
            if data.get("error"):
                print(f"LLM error on {model}: {str(data['error'])[:80]}")
                return None, True
            result = extract_llm_response(data)
            if result:
                return result, False
            print(f"Empty response from {model}")
            return None, True
    except Exception as e:
        print(f"LLM call failed on {model}: {type(e).__name__}: {str(e)[:60]}")
        return None, True

async def call_ollama_direct(prompt: str) -> Optional[str]:
    try:
        async with httpx.AsyncClient(timeout=OLLAMA_DIRECT_TIMEOUT) as client:
            resp = await client.post(
                f"{OLLAMA_DIRECT}/api/chat",
                json={
                    "model": OLLAMA_DIRECT_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": False,
                    "options": {"num_predict": 80}
                }
            )
            data = resp.json()
            content = data.get("message", {}).get("content", "").strip()
            if content:
                return content
            return None
    except Exception as e:
        print(f"Ollama direct failed: {type(e).__name__}: {str(e)[:60]}")
        return None

async def get_llm_response(prompt: str) -> Optional[str]:
    for model in MODELS:
        result, should_retry = await call_llm(prompt, model)
        if result:
            return result
        if not should_retry:
            break
    
    ollama_result = await call_ollama_direct(prompt)
    if ollama_result:
        print(f"Ollama direct fallback worked")
        return ollama_result
    
    fallback = "Hallo! 👋 Leuk dat je contact opneemt met Belinus! Ik help je graag. Stel gerust je vraag!"
    print(f"All models failed, using friendly fallback")
    return fallback

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

        ai_response = await get_llm_response(prompt)
        handoff = "[TRANSFER]" in ai_response
        ai_response = ai_response.replace("[TRANSFER]", "").strip()
        
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

    ai_response = await get_llm_response(prompt)
    return {"answer": ai_response, "context_used": bool(ctx), "cached": False}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=15579)

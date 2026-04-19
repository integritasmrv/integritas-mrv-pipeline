from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import Optional, Tuple
import asyncio
import time
import re
import hashlib
import httpx
import os
import urllib3
from hatchet import Hatchet
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = FastAPI(title="IntegritasMRV Chat + RAG")

HATCHET_TOKEN = os.environ.get("HATCHET_CLIENT_TOKEN", "eyJhbGciOiJFUzI1NiIsImtpZCI6IkRFOWxydyJ9.eyJhdWQiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCJleHAiOjE3ODQzMTI1MzQsImdycGNfYnJvYWRjYXN0X2FkZHJlc3MiOiJoYXRjaGV0LWVuZ2luZTo3MDcwIiwiaWF0IjoxNzc2NTM2NTM0LCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCJzZXJ2ZXJfdXJsIjoiaHR0cDovL2xvY2FsaG9zdDo4MDgwIiwic3ViIjoiNzA3ZDA4NTUtODBhYi00ZTFmLWExNTYtZjFjNDU0NmNiZjUyIiwidG9rZW5faWQiOiI1Y2NhNTU1MS03ODYwLTQxYTQtODMzZC1lNTg0NTQ3YTM4MjAifQ.V6kV3M5OZB5xHhjtrIOCEs_rif78GhW5_yno6q9qnJgO4dCRnqY8UAgERVert3XYmgv5sf_g7_hhq_xjoDpisw")
HATCHET_HOST = os.environ.get("HATCHET_CLIENT_HOST_PORT", "10.0.20.1:7070")

try:
    hatchet_client = Hatchet(
        namespace="default",
        token=HATCHET_TOKEN,
        port=HATCHET_HOST
    )
    print(f"[HATCHET] Client initialized: {hatchet_client.engine().url}")
except Exception as e:
    print(f"[HATCHET] Failed to initialize: {e}")
    hatchet_client = None

REDIS_HOST = "10.0.4.8"
REDIS_PORT = 6379
CACHE_TTL = 3600

OLLAMA_URL = "http://10.0.4.10:11434"
OLLAMA_MODEL = "llama3.2:3b"
OLLAMA_TIMEOUT = 30.0

DIRECT_PATTERNS = [
    r"^(hi|hello|hey|bonjour|salut|hallo|goededag|goeie|bonsoir|hoi|goedendag)$",
    r"^(merci|thanks|thank you|dank u|danke|bedankt|thx|gracias)$",
    r"^(ok|okay|yes|no|oui|non|ja|yea|yeah|jep)$",
    r"^(goodbye|bye|tot ziens|à bientôt|adieu|tot straks)$",
    r"^(how are you|how do you do|ça va|hoe gaat het)$",
    r"^(what is your name|who are you|wie ben je|wie zijn jullie)$",
    r"^(see you|later|à plus)$",
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
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=1, socket_timeout=2)
        cached = r.get(key)
        return cached.decode() if cached else None
    except:
        return None

async def set_cached(key: str, value: str) -> None:
    try:
        import redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=1, socket_timeout=2)
        r.setex(key, CACHE_TTL, value)
    except:
        pass

async def query_ollama(prompt: str, system: str = "") -> str:
    try:
        async with httpx.AsyncClient(timeout=OLLAMA_TIMEOUT) as client:
            payload = {
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False
            }
            if system:
                payload["system"] = system
            resp = await client.post(f"{OLLAMA_URL}/api/generate", json=payload)
            if resp.status_code == 200:
                return resp.json().get("response", "").strip()
    except Exception as e:
        print(f"[OLLAMA] Error: {e}")
    return "Sorry, I couldn't generate a response."

async def query_rag(query: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            rag_resp = await client.post(
                "http://intelligence-lightrag:9621/query/data",
                json={"query": query, "mode": "hybrid"},
                headers={"LIGHTRAG-WORKSPACE": "poweriq"}
            )
            if rag_resp.status_code == 200:
                data = rag_resp.json()
                return data.get("choices", [{}])[0].get("message", {}).get("content", "")
    except Exception as e:
        print(f"RAG error: {e}")
    return ""

SYSTEM_PROMPT = """Je spreekt met Belinus, een Belgisch bedrijf gespecialiseerd in thuisbatterijen en zonne-energie oplossingen. 
Je helpt klanten met informatie over onze producten, installaties en energieoplossingen.
Beantwoord kort en behulpzaam in het Nederlands of Engels."""

BELINUS_KEYWORDS = ["batterij", "thuisbatterij", "zonne", "zonnepanelen", "energie", "solar", "opslag", "laadpaal", "warmtepomp", "groene energie", "grid", "back-up", "ev", "electric", "vehicle", "charging", "wallbox", "photovoltaic", "pv", "inverter", "omvormer", "installatie", "monteur", "offerte", "prijs", "kost", "subsidie", "premie", "mvg", "veolia", "fluvius"]

def is_belinus_question(query: str) -> bool:
    q = query.lower()
    return any(kw in q for kw in BELINUS_KEYWORDS)

async def chat_with_llm(message: str, use_rag: bool = True) -> str:
    if is_belinus_question(message) and use_rag:
        context = await query_rag(message)
        if context:
            prompt = f"""Context over Belinus producten:
{context}

Klant vraag: {message}

Antwoord als Belinus medewerker in het Nederlands of Engels:"""
            return await query_ollama(prompt, SYSTEM_PROMPT)
    return await query_ollama(message, SYSTEM_PROMPT)

class WebformRequest(BaseModel):
    first_name: str
    last_name: str
    email: str
    phone: str = ""
    company: str = ""
    message: str = ""

class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None

class Meta(BaseModel):
    session_id: str

class AskRequest(BaseModel):
    query: str
    stream: bool = True

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/ingest/webform")
async def ingest_webform(request: Request):
    try:
        body = await request.json()
        first_name = body.get("first-name", "")
        last_name = body.get("last-name", "")
        email = body.get("your-email") or body.get("email", "unknown")
        phone = body.get("phone", "")
        company = body.get("company", "")
        
        wf_id = f"webform-{email}-{int(time.time())}"
        
        message = f"Name: {first_name} {last_name}, Email: {email}, Phone: {phone}, Company: {company}, Message: {body.get('message', '')}"
        
        if hatchet_client:
            try:
                hatchet_client.event_push("cf7-lead", {"message": message})
                print(f"[HATCHET] Pushed cf7-lead via SDK: {wf_id}")
                return {"status": "accepted", "workflow_id": wf_id, "engine": "hatchet"}
            except Exception as he:
                print(f"[HATCHET] SDK error: {he}")
                return {"status": "error", "detail": f"Hatchet SDK error: {str(he)[:100]}"}
        else:
            return {"status": "error", "detail": "Hatchet client not initialized"}
    except Exception as e:
        print(f"Webform error: {e}")
        return {"status": "error", "detail": str(e)[:100]}

@app.post("/webhook/hubspot")
async def webhook_hubspot(request: Request):
    try:
        body = await request.json()
        print(f"[WEBHOOK] HubSpot event received", flush=True)
        
        email = body.get("properties", {}).get("email", {}).get("value", "")
        firstname = body.get("properties", {}).get("firstname", {}).get("value", "")
        lastname = body.get("properties", {}).get("lastname", {}).get("value", "")
        company = body.get("properties", {}).get("company", {}).get("value", "")
        phone = body.get("properties", {}).get("phone", {}).get("value", "")
        
        business_key = body.get("objectId") or "unknown"
        
        if hatchet_client:
            try:
                hatchet_client.event_push("hubspot-sync", {
                    "email": email,
                    "firstname": firstname,
                    "lastname": lastname,
                    "company": company,
                    "phone": phone
                })
                print(f"[HATCHET] Pushed hubspot-sync via SDK: {business_key}")
                return {"status": "accepted", "workflow_id": f"hubspot-{business_key}", "engine": "hatchet"}
            except Exception as he:
                print(f"[HATCHET] SDK error: {he}")
                return {"status": "error", "detail": f"Hatchet SDK error: {str(he)[:100]}"}
        else:
            return {"status": "error", "detail": "Hatchet client not initialized"}
    except Exception as e:
        print(f"HubSpot webhook error: {e}")
        return {"status": "error", "detail": str(e)}

@app.get("/chatwoot/webhook")
async def chatwoot_verify():
    return {"status": "ok"}

@app.post("/chatwoot/webhook")
async def chatwoot_webhook(request: Request):
    try:
        body = await request.json()
        msg_type = body.get("message_type")
        content = body.get("content", "")
        sender = body.get("sender", {})
        
        print(f"[CHATWOOT] {msg_type}: {content[:50]}...", flush=True)
        
        if msg_type == "incoming":
            session_id = f"chatwoot-{sender.get('id', 'unknown')}"
            
            cached = await get_cached(cache_key_fn(session_id, "session"))
            if not cached:
                use_rag = needs_retrieval(content)
                reply = await chat_with_llm(content, use_rag)
                await set_cached(cache_key_fn(session_id, "session"), reply)
                
                return {
                    "message": {
                        "content": reply,
                        "message_type": "outgoing"
                    }
                }
            else:
                return {"status": "cached"}
        
        return {"status": "ignored"}
    except Exception as e:
        print(f"Chatwoot error: {e}")
        return {"status": "error"}

@app.post("/ask")
async def ask(request: AskRequest, meta: Meta):
    query = request.query
    session_id = meta.session_id
    
    if not needs_retrieval(query):
        reply = await query_ollama(query, SYSTEM_PROMPT)
        return {"answer": reply, "session_id": session_id}
    
    cached = await get_cached(cache_key_fn(query))
    if cached:
        print(f"[CACHE] Hit for: {query[:30]}...")
        return {"answer": cached, "session_id": session_id, "cached": True}
    
    reply = await chat_with_llm(query)
    await set_cached(cache_key_fn(query), reply)
    
    return {"answer": reply, "session_id": session_id}
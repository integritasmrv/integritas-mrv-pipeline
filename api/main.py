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
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = FastAPI(title="IntegritasMRV Chat + RAG")

HATCHET_TOKEN = os.environ.get("HATCHET_CLIENT_TOKEN", "eyJhbGciOiJFUzI1NiIsImtpZCI6IkRFOWxydyJ9.eyJhdWQiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCJleHAiOjE3ODQzMTI1MzQsImdycGNfYnJvYWRjYXN0X2FkZHJlc3MiOiJoYXRjaGV0LWVuZ2luZTo3MDcwIiwiaWF0IjoxNzc2NTM2NTM0LCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCJzZXJ2ZXJfdXJsIjoiaHR0cDovL2xvY2FsaG9zdDo4MDgwIiwic3ViIjoiNzA3ZDA4NTUtODBhYi00ZTFmLWExNTYtZjFjNDU0NmNiZjUyIiwidG9rZW5faWQiOiI1Y2NhNTU1MS03ODYwLTQxYTQtODMzZC1lNTg0NTQ3YTM4MjAifQ.V6kV3M5OZB5xHhjtrIOCEs_rif78GhW5_yno6q9qnJgO4dCRnqY8UAgERVert3XYmgv5sf_g7_hhq_xjoDpisw")
HATCHET_HOST = "144.91.126.111:7070"

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
        pass
    return None

async def set_cached(key: str, value: str):
    try:
        import redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=1, socket_timeout=2)
        r.setex(key, CACHE_TTL, value)
    except:
        pass

async def call_ollama(prompt: str) -> Optional[str]:
    try:
        async with httpx.AsyncClient(timeout=OLLAMA_TIMEOUT) as client:
            resp = await client.post(
                f"{OLLAMA_URL}/api/generate",
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "num_predict": 150,
                        "temperature": 0.3,
                        "top_p": 0.9
                    }
                }
            )
            data = resp.json()
            content = data.get("response", "").strip()
            if content:
                return content
            return None
    except Exception as e:
        print(f"Ollama call failed: {type(e).__name__}: {str(e)[:80]}")
        return None

def get_rule_based_response(query: str) -> Optional[str]:
    q = query.lower()
    if any(k in q for k in ['batterij','battery','opslag','storage','energywall']):
        return "De Energywall G1 is lithium-vrij met 50000 laadcycli en 10 jaar garantie! Specs: 99% efficiëntie, modulair van 5kWh tot 500kWh."
    if any(k in q for k in ['prijs','price','kost','kosten','offerte','quotation']):
        return "Voor een offerte op maat, neem contact op via info@belinus.net of bel +32 XX XXX XX XX"
    if any(k in q for k in ['contact','bereik','email','telefoon','phone','reach']):
        return "Je kunt ons bereiken via info@belinus.net of bel +32 XX XXX XX XX"
    if any(k in q for k in ['bedankt','dank','thanks','merci','graag','thx']):
        return "Graag gedaan! Nog een vraag over onze batterijsystemen?"
    if 'lithium' in q:
        return "De Belinus batterij is lithium-VRIJ! We gebruiken grafeen supercapacitor technologie - duurzamer en veiliger dan lithium."
    if any(k in q for k in ['garantie','warranty','jaar','year']):
        return "Onze Energywall G1 komt met 10 jaar garantie! Dat is de beste in de industrie."
    if any(k in q for k in ['cyclus','cycli','cycle','cycles']):
        return "De Energywall G1 gaat tot 50000 laadcycli mee! Dat is 10x meer dan lithium batterijen."
    if any(k in q for k in ['hallo','hi','hello','hoi','hey','goede','goedendag']):
        return "Hallo! Welkom bij Belinus! Waarmee kan ik je helpen?"
    if any(k in q for k in ['zon','zonne','solar','sun']):
        return "Ja! De Energywall G1 integreert perfect met zonnepanelen voor energieopslag."
    if any(k in q for k in ['effici','efficientie','efficiency']):
        return "De Energywall G1 heeft 99% efficiëntie - een van de hoogste in de industrie!"
    if any(k in q for k in ['modulair','modular','schaalbaar','scalable']):
        return "Ons systeem is modulair van 5kWh tot 500kWh - perfect voor thuis of bedrijf!"
    if any(k in q for k in ['belg','belgi','belgie']):
        return "Belinus is een Belgisch bedrijf! We leveren door heel Europa."
    if any(k in q for k in ['duurzaam','duurzame','sustainable','groen','green']):
        return "Onze lithium-vrije technologie is duurzamer en veiliger voor het milieu!"
    if any(k in q for k in ['veilig','veiligheid','safe','safety']):
        return "Veiligheid eerst! Onze grafeen supercapacitor batterijen zijn brandveilig en niet explosief."
    if any(k in q for k in ['install','installatie',' монтаж']):
        return "We bieden professionele installatie. Neem contact op voor meer info via info@belinus.net"
    if any(k in q for k in ['oppervlak','dak','roof','surface']):
        return "Onze batterijsystemen zijn compact en geschikt voor diverse installatielocaties."
    return None

async def get_llm_response(prompt: str, query: str = "") -> str:
    if query:
        rule_response = get_rule_based_response(query)
        if rule_response:
            return rule_response
    
    ollama_result = await call_ollama(prompt)
    if ollama_result:
        return ollama_result
    
    if query:
        rule_response = get_rule_based_response(query)
        if rule_response:
            return rule_response
    
    return "Hallo! Ik help je graag met vragen over onze lithium-vrije batterijsystemen. Stel gerust je vraag!"

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
        
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            r = await client.post(
                "https://hatchet.integritasmrv.com/api/v1/events",
                headers={
                    "Authorization": f"Bearer {HATCHET_TOKEN}",
                    "Content-Type": "application/json"
                },
                json={
                    "key": "cf7-lead",
                    "payload": {"message": message}
                }
            )
            print(f"[HATCHET] Response: {r.status_code}")
        
        if r.status_code in (200, 201, 202):
            print(f"[HATCHET] Pushed cf7-lead: {wf_id}")
            return {"status": "accepted", "workflow_id": wf_id, "engine": "hatchet"}
        else:
            return {"status": "error", "detail": f"Hatchet error: {r.status_code}"}
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
        
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            r = await client.post(
                "https://hatchet.integritasmrv.com/api/v1/events",
                headers={
                    "Authorization": f"Bearer {HATCHET_TOKEN}",
                    "Content-Type": "application/json"
                },
                json={
                    "key": "hubspot-sync",
                    "payload": {
                        "email": email,
                        "firstname": firstname,
                        "lastname": lastname,
                        "company": company,
                        "phone": phone
                    }
                }
            )
            if r.status_code in (200, 201, 202):
                print(f"[HATCHET] Pushed hubspot-sync event: {business_key}")
                return {"status": "accepted", "workflow_id": f"hubspot-{business_key}", "engine": "hatchet"}
            else:
                print(f"[HATCHET] Error: {r.status_code} - {r.text}")
                return {"status": "error", "detail": f"Hatchet API error: {r.status_code}"}
    except Exception as e:
        print(f"HubSpot webhook error: {e}")
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

        ai_response = await get_llm_response(prompt, user_message)
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
    
    prompt = f"""Je bent een behulpzame en vriendelijke verkoopassistent voor Belinus, een Belgisch bedrijf dat lithium-vrije batterijopslagsystemen maakt.

Producten:
- Energywall G1: Lithium-vrije thuisbatterij met grafeen supercapacitor technologie
- 50000 laadcycli, 99% efficiëntie, 10 jaar garantie
- Modulair: 5kWh tot 500kWh

Context: {ctx if ctx else 'Geen specifieke context'}
Vraag: {query}
Antwoord kort en behulpzaam:"""

    ai_response = await get_llm_response(prompt, query)
    return {"answer": ai_response, "context_used": bool(ctx), "cached": False}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=15579)
from fastapi import FastAPI, Request
from pydantic import BaseModel
from typing import Optional
import asyncio
import time
import re
import hashlib
import httpx
import os
import asyncpg

app = FastAPI(title="IntegritasMRV Chat + RAG")

HS_PAT = os.environ.get("HUBSPOT_API_TOKEN", "") or os.environ.get("HUBSPOT_SYNC_TOKEN", "")
HATCHET_TOKEN = os.environ.get("HATCHET_CLIENT_TOKEN", "")
HATCHET_HOST = os.environ.get("HATCHET_CLIENT_HOST_PORT", "10.0.20.1:7070")
HATCHET_REST_URL = f"http://{HATCHET_HOST}/api/v1/events"
print(f"[HATCHET] Using HTTP REST - {HATCHET_REST_URL}")

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

CRM_HOSTS = {
    "integritasmrv": {
        "host": os.environ.get("CRM_INTEGRITASMRV_HOST", "10.0.13.2"),
        "port": int(os.environ.get("CRM_INTEGRITASMRV_PORT", "5432")),
        "user": "integritasmrv_crm_user",
        "password": "oYxxPKRfAHAD263VSDcKmljKY0vInx2QTl6PooKoqmmiDops",
        "database": "integritasmrv_crm",
    },
    "poweriq": {
        "host": os.environ.get("CRM_POWERIQ_HOST", "10.0.14.2"),
        "port": int(os.environ.get("CRM_POWERIQ_PORT", "5432")),
        "user": "poweriq_crm_user",
        "password": "P0w3r1Q_CRM_S3cur3_P@ss_2026",
        "database": "poweriq_crm",
    },
}


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


async def get_hubspot_owner_email(owner_id: str) -> str:
    if not owner_id or not HS_PAT:
        return ""
    try:
        async with httpx.AsyncClient(timeout=5.0) as c:
            r = await c.get(
                f"https://api.hubapi.com/crm/v3/owners/{owner_id}",
                headers={"Authorization": f"Bearer {HS_PAT}", "Content-Type": "application/json"},
            )
            if r.status_code == 200:
                data = r.json()
                return data.get("email", "")
    except Exception as e:
        print(f"[HUBSPOT] Owner lookup failed for {owner_id}: {e}")
    return ""


async def resolve_owner_email(evt: dict) -> str:
    props = evt.get("properties", {})
    direct_email = props.get("hubspot_owner_email", {}).get("value", "")
    if direct_email:
        return direct_email
    owner_id = (
        evt.get("subscription", {}).get("ownerId") or
        props.get("hubspot_owner_id", {}).get("value") or
        ""
    )
    if owner_id and HS_PAT:
        try:
            return await asyncio.wait_for(get_hubspot_owner_email(owner_id), timeout=5.0)
        except asyncio.TimeoutError:
            print(f"[HUBSPOT] Owner lookup timed out for {owner_id}")
    return ""


async def get_crm_for_owner(owner_email: str) -> tuple[str, dict]:
    email_lower = str(owner_email).lower()
    if "syncpower@" in email_lower:
        return "poweriq", CRM_HOSTS["poweriq"]
    return "integritasmrv", CRM_HOSTS["integritasmrv"]


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

        if not HATCHET_TOKEN:
            return {"status": "error", "detail": "HATCHET_CLIENT_TOKEN not set"}

        try:
            async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
                r = await client.post(
                    HATCHET_REST_URL,
                    headers={"Authorization": f"Bearer {HATCHET_TOKEN}", "Content-Type": "application/json"},
                    json={"key": "cf7-lead", "payload": {"message": message}}
                )
                print(f"[HATCHET] Response: {r.status_code} - {r.text[:100]}")
                if r.status_code in (200, 201, 202):
                    print(f"[HATCHET] Pushed cf7-lead via REST: {wf_id}")
                    return {"status": "accepted", "workflow_id": wf_id, "engine": "hatchet"}
                else:
                    return {"status": "error", "detail": f"Hatchet error: {r.status_code}"}
        except Exception as he:
            print(f"[HATCHET] REST error: {he}")
            return {"status": "error", "detail": f"Hatchet REST error: {str(he)[:100]}"}
    except Exception as e:
        print(f"Webform error: {e}")
        return {"status": "error", "detail": str(e)[:100]}


@app.post("/webhook/hubspot")
async def webhook_hubspot(request: Request):
    try:
        body = await request.json()
        events = body if isinstance(body, list) else [body]
        print(f"[WEBHOOK] HubSpot: {len(events)} event(s)", flush=True)

        results = []

        for evt in events:
            try:
                props = evt.get("properties", {})
                ot = evt.get("objectType", "contact").lower()
                hs_id = str(evt.get("objectId", ""))

                owner_email = await resolve_owner_email(evt)
                crm_name, cfg = await get_crm_for_owner(owner_email)
                cfg["ssl"] = None

                conn = await asyncpg.connect(**cfg)

                if "company" in ot:
                    nv = props.get("name", props.get("company", {}))
                    name = nv.get("value", "") if isinstance(nv, dict) else str(nv or "")
                    if not name:
                        name = "Company " + hs_id

                    ex = await conn.fetchrow("SELECT id FROM nb_crm_customers WHERE hubspot_id = $1", hs_id)
                    if ex:
                        await conn.execute('UPDATE nb_crm_customers SET enrichment_status = $2, "updatedAt" = NOW() WHERE hubspot_id = $1', hs_id, "To Be Enriched")
                        results.append({"action": "updated", "crm": crm_name, "type": "company", "id": ex["id"]})
                    else:
                        row = await conn.fetchrow('INSERT INTO nb_crm_customers (name, hubspot_id, enrichment_status, "updatedAt", "createdAt") VALUES ($1, $2, $3, NOW(), NOW()) RETURNING id', name, hs_id, "To Be Enriched")
                        results.append({"action": "created", "crm": crm_name, "type": "company", "id": row["id"] if row else None})
                else:
                    fv = props.get("firstname", {})
                    lv = props.get("lastname", {})
                    fn2 = fv.get("value", "") if isinstance(fv, dict) else str(fv or "")
                    ln2 = lv.get("value", "") if isinstance(lv, dict) else str(lv or "")
                    full = (fn2 + " " + ln2).strip() or "Unknown"
                    ev2 = props.get("email", {})
                    email = ev2.get("value", "") if isinstance(ev2, dict) else str(ev2 or "")

                    ex = await conn.fetchrow("SELECT id FROM nb_crm_contacts WHERE hubspot_id = $1", hs_id)
                    if ex:
                        await conn.execute('UPDATE nb_crm_contacts SET enrichment_status = $2, "updatedAt" = NOW() WHERE hubspot_id = $1', hs_id, "To Be Enriched")
                        results.append({"action": "updated", "crm": crm_name, "type": "contact", "id": ex["id"]})
                    else:
                        row = await conn.fetchrow('INSERT INTO nb_crm_contacts (name, email, hubspot_id, enrichment_status, "updatedAt", "createdAt") VALUES ($1, $2, $3, $4, NOW(), NOW()) RETURNING id', full, email or None, hs_id, "To Be Enriched")
                        results.append({"action": "created", "crm": crm_name, "type": "contact", "id": row["id"] if row else None})

                await conn.close()

            except Exception as ee:
                print(f"[WEBHOOK] Event error: {ee}")
                results.append({"error": str(ee)[:100]})

        return {"status": "processed", "results": results}

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

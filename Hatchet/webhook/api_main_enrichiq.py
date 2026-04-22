from fastapi import FastAPI, Request
from pydantic import BaseModel
from typing import Optional
import asyncio
import time
import re
import hashlib
import httpx
import os
import json
import asyncpg

app = FastAPI(title="IntegritasMRV Chat + RAG")

HS_PAT = os.environ.get("HUBSPOT_API_TOKEN", "") or os.environ.get("HUBSPOT_SYNC_TOKEN", "")
HATCHET_TOKEN = os.environ.get(
    "HATCHET_CLIENT_TOKEN",
    "eyJhbGciOiJFUzI1NiIsImtpZCI6ImZ5cTd3QSJ9.eyJhdWQiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCJleHAiOjE3ODQzNjU5MTUsImdycGNfYnJvYWRjYXN0X2FkZHJlc3MiOiJoYXRjaGV0LWVuZ2luZTo3MDcwIiwiaWF0IjoxNzc2NTg5OTE1LCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCJzZXJ2ZXJfdXJsIjoiaHR0cDovL2xvY2FsaG9zdDo4MDgwIiwic3ViIjoiNzA3ZDA4NTUtODBhYi00ZTFmLWExNTYtZjFjNDU0NmNiZjUyIiwidG9rZW5faWQiOiJiMDc5MTc4Zi02OGQ4LTRhMzQtYThlYy1kNzQ2YjE4OGZkMzcifQ.FKSllU33io3P3Dl4bsWvdxPQzHCnXmKpU3PK_vx27DhQvaYjMmFzB91UT2Jw82mvcolXtNBK9tSNP_KcabHCKw",
)
HATCHET_HOST = os.environ.get("HATCHET_CLIENT_HOST_PORT", "217.76.59.199:7070")
HATCHET_TLS = os.environ.get("HATCHET_CLIENT_TLS_STRATEGY", "none")

os.environ["HATCHET_CLIENT_TOKEN"] = HATCHET_TOKEN
os.environ["HATCHET_CLIENT_HOST_PORT"] = HATCHET_HOST
os.environ["HATCHET_CLIENT_TLS_STRATEGY"] = HATCHET_TLS

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

BUSINESS_KEY_MAP = {
    "integritasmrv": "integritasmrv",
    "poweriq": "poweriq",
    "airbnb": "airbnb",
    "ev-batteries": "ev-batteries",
    "private": "private",
}

OWNER_EMAIL_TO_BUSINESS = {
    "synchmrv@": "integritasmrv",
    "syncpower@": "poweriq",
    "syncairbnb@": "airbnb",
    "syncev@": "ev-batteries",
    "syncprivate@": "private",
}

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

ENRICHMENT_DB = {
    "host": os.environ.get("ENRICHMENT_DB_HOST", "10.0.12.9"),
    "port": int(os.environ.get("ENRICHMENT_DB_PORT", "5432")),
    "user": os.environ.get("ENRICHMENT_DB_USER", "admin"),
    "password": os.environ.get("ENRICHMENT_DB_PASS", "Milanmanon987@"),
    "database": os.environ.get("ENRICHMENT_DB_NAME", "enrichment_ev_batteries"),
}


def get_business_key(owner_email: str) -> str:
    email_lower = str(owner_email).lower()
    for prefix, business in OWNER_EMAIL_TO_BUSINESS.items():
        if prefix in email_lower:
            return business
    return "integritasmrv"


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
            payload = {"model": OLLAMA_MODEL, "prompt": prompt, "stream": False}
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
                headers={"LIGHTRAG-WORKSPACE": "poweriq"},
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

BELINUS_KEYWORDS = [
    "batterij", "thuisbatterij", "zonne", "zonnepanelen", "energie", "solar",
    "opslag", "laadpaal", "warmtepomp", "groene energie", "grid", "back-up",
    "ev", "electric", "vehicle", "charging", "wallbox", "photovoltaic", "pv",
    "inverter", "omvormer", "installatie", "monteur", "offerte", "prijs",
    "kost", "subsidie", "premie", "mvg", "veolia", "fluvius",
]


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
        evt.get("subscription", {}).get("ownerId")
        or props.get("hubspot_owner_id", {}).get("value")
        or ""
    )
    if owner_id and HS_PAT:
        try:
            return await asyncio.wait_for(get_hubspot_owner_email(owner_id), timeout=5.0)
        except asyncio.TimeoutError:
            print(f"[HUBSPOT] Owner lookup timed out for {owner_id}")
    return ""


_hatchet = None


async def get_hatchet():
    global _hatchet
    if _hatchet is None:
        from hatchet_sdk import Hatchet
        _hatchet = Hatchet()
    return _hatchet


async def get_enrichment_db_conn():
    return await asyncpg.connect(
        host=ENRICHMENT_DB["host"],
        port=ENRICHMENT_DB["port"],
        user=ENRICHMENT_DB["user"],
        password=ENRICHMENT_DB["password"],
        database=ENRICHMENT_DB["database"],
        ssl=None,
        timeout=10,
    )


async def upsert_entity(
    conn: asyncpg.Connection,
    entity_type: str,
    label: str,
    business_key: str,
    source_system: str,
    external_ids: dict,
) -> str:
    external_ids_json = json.dumps(external_ids) if external_ids else "{}"
    existing = await conn.fetchrow(
        "SELECT id FROM entities WHERE label = $1 AND business_key = $2 AND entity_type = $3",
        label, business_key, entity_type,
    )
    if existing:
        await conn.execute(
            "UPDATE entities SET enrichment_status = 'pending', source_system = $4, external_ids = $5, updated_at = NOW() WHERE id = $1",
            str(existing["id"]), source_system, external_ids_json,
        )
        return str(existing["id"])
    else:
        row = await conn.fetchrow(
            "INSERT INTO entities (entity_type, label, business_key, source_system, external_ids, enrichment_status) VALUES ($1, $2, $3, $4, $5, 'pending') RETURNING id",
            entity_type, label, business_key, source_system, external_ids_json,
        )
        return str(row["id"])


async def push_hatchet_event(key: str, payload: dict) -> str:
    try:
        hatchet = await get_hatchet()
        result = hatchet.event.push(key, payload)
        return getattr(result, "eventId", str(result))
    except Exception as e:
        print(f"[HATCHET] Push failed: {e}")
        return ""


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

        event_id = await push_hatchet_event("cf7-lead", {"message": message})
        print(f"[HATCHET] Pushed cf7-lead: {wf_id} event_id={event_id}")
        return {"status": "accepted", "workflow_id": wf_id, "event_id": event_id}
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
                business_key = get_business_key(owner_email)
                crm_name = "poweriq" if business_key == "poweriq" else "integritasmrv"
                cfg = CRM_HOSTS[crm_name].copy()
                cfg["ssl"] = None

                conn = await asyncpg.connect(**cfg)

                external_ids = {"hubspot_id": hs_id}
                entity_type = "company" if "company" in ot else "contact"

                if "company" in ot:
                    nv = props.get("name", props.get("company", {}))
                    name = nv.get("value", "") if isinstance(nv, dict) else str(nv or "")
                    if not name:
                        name = "Company " + hs_id

                    ex = await conn.fetchrow(
                        "SELECT id FROM nb_crm_customers WHERE hubspot_id = $1", hs_id
                    )
                    if ex:
                        await conn.execute(
                            'UPDATE nb_crm_customers SET enrichment_status = $2, "updatedAt" = NOW() WHERE hubspot_id = $1',
                            hs_id, "To Be Enriched",
                        )
                        crm_entity_id = str(ex["id"])
                        results.append({"action": "updated", "crm": crm_name, "type": "company", "id": crm_entity_id})
                    else:
                        row = await conn.fetchrow(
                            'INSERT INTO nb_crm_customers (name, hubspot_id, enrichment_status, "updatedAt", "createdAt") VALUES ($1, $2, $3, NOW(), NOW()) RETURNING id',
                            name, hs_id, "To Be Enriched",
                        )
                        crm_entity_id = str(row["id"]) if row else ""
                        results.append({"action": "created", "crm": crm_name, "type": "company", "id": crm_entity_id})
                else:
                    fv = props.get("firstname", {})
                    lv = props.get("lastname", {})
                    fn2 = fv.get("value", "") if isinstance(fv, dict) else str(fv or "")
                    ln2 = lv.get("value", "") if isinstance(lv, dict) else str(lv or "")
                    full = (fn2 + " " + ln2).strip() or "Unknown"
                    ev2 = props.get("email", {})
                    email = ev2.get("value", "") if isinstance(ev2, dict) else str(ev2 or "")

                    ex = await conn.fetchrow(
                        "SELECT id FROM nb_crm_contacts WHERE hubspot_id = $1", hs_id
                    )
                    if ex:
                        await conn.execute(
                            'UPDATE nb_crm_contacts SET enrichment_status = $2, "updatedAt" = NOW() WHERE hubspot_id = $1',
                            hs_id, "To Be Enriched",
                        )
                        crm_entity_id = str(ex["id"])
                        results.append({"action": "updated", "crm": crm_name, "type": "contact", "id": crm_entity_id})
                    else:
                        row = await conn.fetchrow(
                            'INSERT INTO nb_crm_contacts (name, email, hubspot_id, enrichment_status, "updatedAt", "createdAt") VALUES ($1, $2, $3, $4, NOW(), NOW()) RETURNING id',
                            full, email or None, hs_id, "To Be Enriched",
                        )
                        crm_entity_id = str(row["id"]) if row else ""
                        results.append({"action": "created", "crm": crm_name, "type": "contact", "id": crm_entity_id})

                await conn.close()

                enrich_conn = await get_enrichment_db_conn()
                entity_id = await upsert_entity(
                    enrich_conn, entity_type, name, business_key, "hubspot", external_ids
                )
                await enrich_conn.close()

                event_key = "enrichment-v2-request" if "company" in ot else "contact-enrichment-v2-request"
                event_payload = {
                    "entity_id": entity_id,
                    "entity_type": entity_type,
                    "business_key": business_key,
                    "source_system": "hubspot",
                    "hubspot_id": hs_id,
                    "crm_name": crm_name,
                    "crm_entity_id": crm_entity_id,
                    "name": name,
                }
                event_id = await push_hatchet_event(event_key, event_payload)
                print(f"[WEBHOOK] Pushed {event_key} entity_id={entity_id} event_id={event_id}")

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
                        "message_type": "outgoing",
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

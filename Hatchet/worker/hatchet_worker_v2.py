import os
import json
import re
import time
import logging
import asyncio
from urllib.parse import urlparse

import httpx
import asyncpg
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [worker-v2] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

from hatchet_sdk import Hatchet, Context, ClientConfig

HATCHET_TOKEN = os.environ.get(
    "HATCHET_CLIENT_TOKEN",
    "eyJhbGciOiJFUzI1NiIsICJraWQiOiJ5MXQyQUEifQ.eyJhdWQiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCAiZXhwIjo0OTMwNzE4ODUwLCAiZ3JwY19icm9hZGNhc3RfYWRkcmVzcyI6ImhhdGNoZXQtZW5naW5lOjcwNzAiLCAiaWF0IjoxNzc3MTE4ODUwLCAiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo4MDgwIiwgInNlcnZlcl91cmwiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCAic3ViIjoiNzA3ZDA4NTUtODBhYi00ZTFmLWExNTYtZjFjNDU0NmNiZjUyIiwgInRva2VuX2lkIjoiYmUwZmViYzgtZjJkMS00YTY3LWI5YTgtODI0ZDIyMmRiYTFhIn0.QegEAhkABe5GdWFEzoJJdnsuvEzHBnCLUTfDa7e4e25zEfsjOEe-TEAzBzmgeKoUdgBTm3H9xJIkrOZSVPxidA",
)
os.environ["HATCHET_CLIENT_TOKEN"] = HATCHET_TOKEN
os.environ.setdefault("HATCHET_CLIENT_HOST_PORT", "hatchet-engine:7070")
os.environ.setdefault("HATCHET_CLIENT_TLS_STRATEGY", "none")

ENRICHMENT_DB_DSN = os.environ.get("ENRICHMENT_DB_DSN", "postgresql://hatchet_user:Hatch3tDB2026!@127.0.0.1:15434/enrichment_ev_batteries")
CRM_INTEGRITASMRV_DSN = os.environ.get("CRM_INTEGRITASMRV_DSN", "postgresql://integritasmrv_crm_user:oYxxPKRfAHAD263VSDcKmljKY0vInx2QTl6PooKoqmmiDops@127.0.0.1:15432/integritasmrv_crm")
CRM_POWERIQ_DSN = os.environ.get("CRM_POWERIQ_DSN", "postgresql://poweriq_crm_user:P0w3r1Q_CRM_S3cur3_P@ss_2026@127.0.0.1:15433/poweriq_crm")
LITELLM_PROXY_URL = os.environ.get("LITELLM_PROXY_URL", "http://127.0.0.1:14000")
LITELLM_PROXY_KEY = os.environ.get("LITELLM_PROXY_KEY", "")
NOMINATIM_URL = os.environ.get("NOMINATIM_SEARCH_URL", "http://127.0.0.1:18180/search")
OVERPASS_URL = os.environ.get("OVERPASS_URL", "https://overpass-api.de/api/interpreter")
SOCIAL_ANALYZER_URL = os.environ.get("SOCIAL_ANALYZER_URL", "http://127.0.0.1:19005")
LIGHTRAG_URL = os.environ.get("LIGHTRAG_BASE_URL", "http://127.0.0.1:19621")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://127.0.0.1:16333")
HS_TOKEN = os.environ.get("HUBSPOT_API_TOKEN", "")
HS_SYNC_URL = os.environ.get("HUBSPOT_SYNC_URL", "")
SEARXNG_URL = os.environ.get("SEARXNG_URL", "http://144.91.126.111:3010")
SCRAPIQ_DISPATCH_URL = os.environ.get("SCRAPIQ_DISPATCH_URL", "")
SCRAPIQ_DB_URL = os.environ.get("SCRAPIQ_DATABASE_URL", "")

MIN_TRUSTED_FIELDS = int(os.environ.get("ENRICHMENT_MIN_TRUSTED_FIELDS", "6"))
MIN_OVERALL_CONFIDENCE = float(os.environ.get("ENRICHMENT_MIN_OVERALL_CONFIDENCE", "0.75"))

BUSINESS_TO_CRM = {"integritasmrv": CRM_INTEGRITASMRV_DSN, "poweriq": CRM_POWERIQ_DSN}
BUSINESS_KEY_TO_SCRAPIQ = {"integritasmrv": "integritas", "poweriq": "pwriq", "airbnb": "airbnb", "private": "private", "ev-batteries": "ev-batteries"}

COMPANY_EXTRACT_FIELDS = ["legal_name", "trade_name", "vat_number", "registration_number", "website", "email", "phone", "street", "city", "postal_code", "country", "industry_code", "linkedin_url", "description", "employee_count", "founded_year", "technologies"]

DIRECTORY_DOMAINS = {"linkedin.com", "crunchbase.com", "pitchbook.com", "wikipedia.org", "facebook.com", "x.com", "twitter.com", "instagram.com", "youtube.com"}

LINKEDIN_BRANCH_HINTS = {"deutschland", "germany", "france", "italy", "spain", "poland", "uk", "usa", "us", "canada", "australia", "india", "japan", "china", "belgium", "netherlands", "mexico", "brazil"}

# === COLLECTORS ===

class RawEvidence:
    def __init__(self, entity_id, collector_name, content_type, raw_content, source_url="", source_weight=0.5, round_number=1, business_key=None, scrapiq_request_id=None, scrapiq_result_id=None):
        self.entity_id = entity_id
        self.collector_name = collector_name
        self.content_type = content_type
        self.raw_content = raw_content
        self.source_url = source_url
        self.source_weight = source_weight
        self.round_number = round_number
        self.business_key = business_key
        self.scrapiq_request_id = scrapiq_request_id
        self.scrapiq_result_id = scrapiq_result_id

async def nominatim_collect(entity, context):
    primary = context.get("primary", {})
    name = str(primary.get("legal_name") or primary.get("label") or entity.get("label", "")).strip()
    city = str(primary.get("city") or primary.get("hq_city", "")).strip()
    country = str(primary.get("country") or primary.get("hq_country", "")).strip()
    query = " ".join(p for p in [name, city, country] if p).strip()
    if not query:
        return []
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(NOMINATIM_URL, params={"q": query, "format": "jsonv2", "addressdetails": 1, "limit": 1, "accept-language": "en"}, headers={"User-Agent": "IntegritasMRV-IntelligenceBot/1.0"})
            resp.raise_for_status()
            rows = resp.json()
    except Exception as e:
        log.warning(f"nominatim failed: {e}")
        return []
    if not rows:
        return []
    hit = rows[0] if isinstance(rows, list) else {}
    addr = hit.get("address") or {}
    structured = {"query": query, "nominatim_display_name": hit.get("display_name"), "lat": str(hit.get("lat") or "").strip(), "lng": str(hit.get("lon") or "").strip(), "address": {"road": addr.get("road", ""), "house_number": addr.get("house_number", ""), "city": addr.get("city") or addr.get("town") or addr.get("village") or addr.get("municipality", ""), "postcode": addr.get("postcode", ""), "country": addr.get("country", ""), "country_code": str(addr.get("country_code", "")).lower()}, "osm_type": hit.get("osm_type"), "osm_id": hit.get("osm_id"), "place_rank": hit.get("place_rank")}
    return [RawEvidence(entity_id=str(entity.get("id")), collector_name="nominatim_geocode", content_type="json", raw_content=json.dumps(structured)[:20000], source_url=NOMINATIM_URL, source_weight=0.95, round_number=int(context.get("round", 1)))]

async def overpass_collect(entity, context):
    primary = context.get("primary", {})
    company_name = str(primary.get("legal_name") or primary.get("label") or entity.get("label", "")).strip()
    if not company_name:
        return []
    def _to_float(v):
        try: return float(str(v).strip())
        except: return None
    lat = _to_float(primary.get("lat") or primary.get("hq_lat"))
    lng = _to_float(primary.get("lng") or primary.get("hq_lng") or primary.get("lon"))
    if lat is None or lng is None:
        parts = [company_name, str(primary.get("city") or primary.get("hq_city", "")).strip(), str(primary.get("country") or primary.get("hq_country", "")).strip()]
        fq = " ".join(p for p in parts if p).strip()
        if fq:
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.get(NOMINATIM_URL, params={"q": fq, "format": "jsonv2", "addressdetails": 1, "limit": 1, "accept-language": "en"}, headers={"User-Agent": "IntegritasMRV-IntelligenceBot/1.0"})
                    resp.raise_for_status()
                    rows = resp.json()
                if rows:
                    lat = _to_float(rows[0].get("lat"))
                    lng = _to_float(rows[0].get("lon"))
            except: pass
    if lat is None or lng is None:
        return []
    safe_name = company_name.replace('"', '').replace('\n', ' ')
    overpass_query = f"[out:json][timeout:25];(node[\"name\"~\"{safe_name}\",i](around:100,{lat},{lng});way[\"name\"~\"{safe_name}\",i](around:100,{lat},{lng});relation[\"name\"~\"{safe_name}\",i](around:100,{lat},{lng}););out body;"
    try:
        async with httpx.AsyncClient(timeout=35) as client:
            resp = await client.post(OVERPASS_URL, data={"data": overpass_query}, headers={"User-Agent": "IntegritasMRV-IntelligenceBot/1.0"})
            resp.raise_for_status()
            payload = resp.json()
    except Exception as e:
        log.warning(f"overpass failed: {e}")
        return []
    elements = payload.get("elements") or []
    results = []
    for el in elements:
        tags = el.get("tags") or {}
        phone = tags.get("phone") or tags.get("contact:phone") or tags.get("telephone")
        website = tags.get("website") or tags.get("contact:website") or tags.get("url")
        if not phone and not website:
            continue
        results.append({"osm_id": el.get("id"), "osm_type": el.get("type"), "name": tags.get("name"), "phone": phone, "website": website, "addr_street": tags.get("addr:street"), "addr_housenumber": tags.get("addr:housenumber"), "addr_city": tags.get("addr:city"), "addr_postcode": tags.get("addr:postcode"), "addr_country": tags.get("addr:country")})
    if not results:
        return []
    return [RawEvidence(entity_id=str(entity.get("id")), collector_name="overpass_lookup", content_type="json", raw_content=json.dumps({"query_name": company_name, "lat": lat, "lng": lng, "results": results})[:20000], source_url=OVERPASS_URL, source_weight=0.75, round_number=int(context.get("round", 1)))]

async def social_analyzer_collect(entity, context):
    primary = context.get("primary", {})
    full_name = (primary.get("full_name") or entity.get("label", "")).strip()
    if not full_name:
        return []
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(25)) as client:
            resp = await client.post(f"{SOCIAL_ANALYZER_URL}/analyze_string", data={"string": full_name, "option": "FindUserProflesFast", "uuid": str(entity.get("id", "unknown"))})
            resp.raise_for_status()
            payload = resp.json()
    except Exception as e:
        log.warning(f"social_analyzer failed: {e}")
        return []
    return [RawEvidence(entity_id=str(entity.get("id")), collector_name="social_analyzer_api", content_type="json", raw_content=json.dumps(payload)[:20000], source_url=f"{SOCIAL_ANALYZER_URL}/analyze_string", source_weight=0.70, round_number=int(context.get("round", 1)), business_key=str(context.get("business_key", "integritasmrv")))]

async def scrapiq_collect(entity, context):
    if not SCRAPIQ_DISPATCH_URL:
        return []
    primary = context.get("primary", {})
    domain = str(primary.get("domain", "")).strip().lower()
    starting_url = f"https://{domain}" if domain else None
    business_key = str(context.get("business_key", "integritasmrv"))
    scrapiq_business = BUSINESS_KEY_TO_SCRAPIQ.get(business_key, "integritas")
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(SCRAPIQ_DISPATCH_URL, json={"request_type": "web_page_discovery", "initiated_by_service": "enrichment-pipeline", "prompt": "\n".join(context.get("queries", [])[:10]) or f"Research {entity.get('label', 'entity')}", "starting_url": starting_url, "search_scope": "full_web", "max_results": 15, "min_relevance_score": 0.5, "summarize_results": True, "llm_alias": "summarizer", "context_entity_type": entity.get("entity_type"), "context_entity_name": entity.get("label"), "context_entity_data": primary, "source_system": "crm-forestfuture", "request_business": scrapiq_business, "enrichment_entity_id": entity.get("id"), "enrichment_round": int(context.get("round", 1))}, timeout=30)
            r.raise_for_status()
            payload = r.json()
    except Exception as e:
        log.warning(f"scrapiq dispatch failed: {e}")
        return []
    if not isinstance(payload, dict) or "request_id" not in payload:
        return [RawEvidence(entity_id=str(entity.get("id")), collector_name="scrapiq_web_discovery", content_type="json", raw_content=json.dumps(payload)[:20000], source_url=SCRAPIQ_DISPATCH_URL, source_weight=0.75, round_number=int(context.get("round", 1)))]
    request_id = int(payload["request_id"])
    if not SCRAPIQ_DB_URL:
        return []
    start = time.time()
    rows = []
    while (time.time() - start) < 900:
        try:
            conn = await asyncpg.connect(SCRAPIQ_DB_URL)
            try:
                row = await conn.fetchrow("SELECT status FROM nb_scrapiq_requests WHERE id=$1", request_id)
                if row and row["status"] in ("finished", "failed"):
                    items = await conn.fetch("SELECT id, found_page_url, page_title, ai_summary, relevance_score FROM nb_scrapiq_result_pages WHERE request_id=$1 AND status='stored' ORDER BY relevance_score DESC", request_id)
                    rows = [dict(i) for i in items]
                    break
            finally:
                await conn.close()
        except: pass
        await asyncio.sleep(15)
    if not rows:
        return [RawEvidence(entity_id=str(entity.get("id")), collector_name="scrapiq_web_discovery", content_type="json", raw_content=json.dumps({"status": "no_results", "request_id": request_id})[:20000], source_url=starting_url, source_weight=0.75, round_number=int(context.get("round", 1)))]
    out = []
    for r in rows:
        out.append(RawEvidence(entity_id=str(entity.get("id")), collector_name="scrapiq_web_discovery", content_type="text", raw_content=f"{r.get('page_title', '')}\n{r.get('ai_summary', '')}\n{r.get('found_page_url', '')}", source_url=r.get("found_page_url"), source_weight=float(r.get("relevance_score") or 50) / 100, round_number=int(context.get("round", 1)), scrapiq_request_id=request_id, scrapiq_result_id=int(r["id"])))
    return out

# === LLM FUNCTIONS ===

async def llm_complete(alias, prompt, json_mode=False, max_tokens=2048, temperature=0.1):
    payload = {"model": alias, "messages": [{"role": "user", "content": prompt}], "max_tokens": max_tokens, "temperature": temperature}
    if json_mode:
        payload["response_format"] = {"type": "json_object"}
    headers = {"Content-Type": "application/json"}
    if LITELLM_PROXY_KEY:
        headers["Authorization"] = f"Bearer {LITELLM_PROXY_KEY}"
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(f"{LITELLM_PROXY_URL}/chat/completions", json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
    return ((data.get("choices") or [{}])[0].get("message") or {}).get("content", "")

# === EXTRACTION HELPERS ===

def _entity_tokens(name):
    return [t for t in re.findall(r"[a-z0-9]+", (name or "").lower()) if len(t) >= 3]

def _domain_is_directory(domain):
    d = (domain or "").lower().replace("www.", "")
    for item in DIRECTORY_DOMAINS:
        if d == item or d.endswith("." + item):
            return True
    return False

def _normalize_linkedin_company_url(url):
    raw = (url or "").strip()
    if not raw:
        return None
    try:
        parsed = urlparse(raw)
    except: return None
    host = (parsed.netloc or "").lower().replace("www.", "")
    if "linkedin.com" not in host:
        return None
    parts = [p for p in (parsed.path or "").split("/") if p]
    if len(parts) < 2 or parts[0].lower() != "company":
        return None
    return f"https://www.linkedin.com/company/{parts[1].strip()}"

def _extract_linkedin_urls(text):
    if not text:
        return []
    pattern = re.compile(r"https?://(?:[a-z]{2,3}\\.)?linkedin\\.com/company/[A-Za-z0-9\\-_%/]+", re.I)
    out = []
    for m in pattern.findall(text):
        cleaned = m.strip().rstrip(".,;:)\"'")
        normalized = _normalize_linkedin_company_url(cleaned)
        if normalized and normalized not in out:
            out.append(normalized)
    return out

def _normalize_field(key, value):
    if value in (None, ""):
        return None
    v = str(value).strip()
    if not v:
        return None
    if key in ("official_website", "linkedin_company_url") and not v.startswith(("http://", "https://")):
        return None
    if key == "website" and not v.startswith(("http://", "https://")):
        return None
    if key == "founded_year":
        m = "".join(ch for ch in v if ch.isdigit())
        return m if len(m) == 4 else None
    if key in ("lat", "lng"):
        try: return str(float(v))
        except: return None
    if key == "country_code":
        cc = re.sub(r"[^a-zA-Z]", "", v).lower()
        return cc if len(cc) == 2 else None
    if key == "phone":
        return re.sub(r"\s+", " ", v)[:120]
    return v[:4000]

def _calibrate_confidence(key, value, confidence, source):
    conf = max(0.0, min(1.0, confidence))
    v = (value or "").lower()
    src = (source or "").lower()
    if key == "official_website":
        if v.startswith("https://"):
            conf = max(conf, 0.8)
        if "heuristic_domain" in src:
            conf = max(conf, 0.95)
    elif key == "linkedin_company_url":
        if "linkedin.com/company/" in v:
            conf = max(conf, 0.92)
    elif key in ("lat", "lng") and "nominatim" in src:
        conf = max(conf, 0.95)
    elif key in ("address", "city", "postcode", "country", "country_code") and "nominatim" in src:
        conf = max(conf, 0.9)
    elif key in ("phone", "website") and "overpass" in src:
        conf = max(conf, 0.82)
    return max(0.0, min(1.0, conf))

def _heuristic_fields_from_evidence(evidence_rows, entity_label, seeded_website):
    out = {}
    domains_count = {}
    linkedin_sources = {}
    entity_tokens = _entity_tokens(entity_label)
    seeded_domain = (urlparse((seeded_website or "")).netloc or "").lower().replace("www.", "")
    for e in evidence_rows:
        src_url = str(e.get("source_url") or "").strip()
        if not src_url:
            continue
        domain = (urlparse(src_url).netloc or "").lower().replace("www.", "")
        if not domain:
            continue
        domains_count[domain] = domains_count.get(domain, 0) + 1
        content = str(e.get("raw_content") or "")
        candidates = []
        norm = _normalize_linkedin_company_url(src_url)
        if norm:
            candidates.append(norm)
        for c in _extract_linkedin_urls(content):
            if c not in candidates:
                candidates.append(c)
        for cand in candidates:
            score = linkedin_sources.get(cand, 0.0) + 2.0
            if any(tok in cand.lower() for tok in entity_tokens):
                score += 2.0
            if seeded_domain and domain == seeded_domain:
                score += 7.0
            linkedin_sources[cand] = score
    if linkedin_sources:
        scored = [(linkedin_sources[u] + 10.0 + (5 if "/life" not in u.lower() and "/jobs" not in u.lower() else 0) + (6 if any(tok in u.lower() for tok in entity_tokens) else 0), u) for u in linkedin_sources]
        scored.sort(key=lambda x: x[0], reverse=True)
        out["linkedin_company_url"] = (scored[0][1], 0.95, "heuristic_linkedin")
    if seeded_website and seeded_website.startswith(("http://", "https://")):
        out["official_website"] = (seeded_website, 0.97, "heuristic_seed")
    if domains_count and "official_website" not in out:
        candidates = [(freq, dom) for dom, freq in domains_count.items() if not _domain_is_directory(dom)]
        if candidates:
            candidates.sort(key=lambda x: x[0], reverse=True)
            domain = candidates[0][1]
            if not seeded_domain or domain != seeded_domain:
                out["official_website"] = (f"https://{domain}", 0.92, "heuristic_domain")
    for e in evidence_rows:
        text = str(e.get("raw_content") or "")[:1200]
        for ln in [l.strip() for l in text.splitlines() if l.strip()]:
            if ln.startswith(("http://", "https://")) or len(ln) < 40 or re.search(r"^[\-*#]+\s*$", ln):
                continue
            out["company_description"] = (ln[:600], 0.62, "heuristic_text")
            break
        if "company_description" in out:
            break
    for e in evidence_rows:
        collector = str(e.get("collector_name", "")).lower()
        raw = str(e.get("raw_content") or "")
        if "nominatim" in collector:
            try:
                payload = json.loads(raw)
                lat = _normalize_field("lat", payload.get("lat"))
                lng = _normalize_field("lng", payload.get("lng"))
                addr = payload.get("address") or {}
                road = str(addr.get("road", "")).strip()
                house = str(addr.get("house_number", "")).strip()
                full_addr = (road + " " + house).strip()
                if lat:
                    out["lat"] = (lat, 0.97, "nominatim_heuristic")
                if lng:
                    out["lng"] = (lng, 0.97, "nominatim_heuristic")
                if full_addr:
                    out["address"] = (full_addr, 0.92, "nominatim_heuristic")
                city = _normalize_field("city", addr.get("city"))
                if city:
                    out["city"] = (city, 0.93, "nominatim_heuristic")
                postcode = _normalize_field("postcode", addr.get("postcode"))
                if postcode:
                    out["postcode"] = (postcode, 0.90, "nominatim_heuristic")
                country = _normalize_field("country", addr.get("country"))
                if country:
                    out["country"] = (country, 0.92, "nominatim_heuristic")
                cc = _normalize_field("country_code", addr.get("country_code"))
                if cc:
                    out["country_code"] = (cc, 0.96, "nominatim_heuristic")
            except: pass
    for e in evidence_rows:
        collector = str(e.get("collector_name", "")).lower()
        raw = str(e.get("raw_content") or "")
        if "overpass" in collector:
            try:
                payload = json.loads(raw)
                rows = payload.get("results") if isinstance(payload, dict) else []
                if isinstance(rows, list):
                    for row in rows[:5]:
                        phone = _normalize_field("phone", row.get("phone"))
                        website = _normalize_field("website", row.get("website"))
                        if phone:
                            out["phone"] = (phone, 0.85, "overpass_heuristic")
                        if website:
                            out["website"] = (website, 0.84, "overpass_heuristic")
            except: pass
    return out

# === CORE ENRICHMENT PIPELINE ===

async def build_context_bundle(entity_id, round_number, business_key):
    conn = await asyncpg.connect(ENRICHMENT_DB_DSN)
    try:
        row = await conn.fetchrow("SELECT id::text AS id, entity_type, label, source_system, external_ids FROM entities WHERE id=$1::uuid AND business_key=$2", entity_id, business_key)
        if not row:
            return {}
        seed = dict(row)
    finally:
        await conn.close()
    conn = await asyncpg.connect(ENRICHMENT_DB_DSN)
    try:
        attrs = await conn.fetch("SELECT attr_key, attr_value, attr_value_json, confidence FROM entity_attributes WHERE entity_id=$1 AND confidence >= 0.7 AND is_trusted=true ORDER BY confidence DESC", entity_id)
        primary = {}
        for r in attrs:
            primary[r["attr_key"]] = r["attr_value_json"] if r["attr_value_json"] is not None else r["attr_value"]
    finally:
        await conn.close()
    if seed:
        primary.setdefault("label", seed.get("label"))
        if seed.get("entity_type") == "company":
            primary.setdefault("legal_name", seed.get("label"))
        ext = seed.get("external_ids") or {}
        if isinstance(ext, str):
            try: ext = json.loads(ext)
            except: ext = {}
        if isinstance(ext, dict):
            if ext.get("domain"):
                primary.setdefault("domain", ext.get("domain"))
            if ext.get("company_name"):
                primary.setdefault("legal_name", ext.get("company_name"))
    name = str(primary.get("legal_name") or primary.get("label") or "company").strip()
    domain = str(primary.get("domain", "")).strip()
    bare_domain = domain.replace("https://", "").replace("http://", "").strip("/")
    deterministic = [f'"{name}" official website', f'"{name}" linkedin company', f'"{name}" crunchbase', f'"{name}" pitchbook', f'"{name}" company profile', f'"{name}" leadership team', f'"{name}" funding', f'"{name}" news', f'"{name}" contact', f'"{name}" headquarters']
    if bare_domain:
        deterministic.insert(0, bare_domain)
        deterministic.insert(1, f'site:{bare_domain} "about"')
    try:
        raw = await llm_complete("query-generator", f"""Generate 10 targeted search queries for enrichment.\nName: {name or 'unknown'}\nDomain: {domain or 'unknown'}\nCity: {primary.get('city') or 'unknown'}\nReturn JSON array only.\n""", json_mode=True, max_tokens=800)
        parsed = json.loads(raw)
        llm_queries = [str(x) for x in parsed] if isinstance(parsed, list) else ([str(x) for x in parsed.get("queries", [])] if isinstance(parsed, dict) else [])
        merged = []
        for q in deterministic + llm_queries:
            q = q.strip()
            if q and q not in merged:
                merged.append(q)
            if len(merged) >= 10:
                break
        queries = merged if merged else deterministic[:10]
    except:
        queries = deterministic[:10]
    return {"entity_id": entity_id, "round": round_number, "primary": primary, "queries": queries, "business_key": business_key}

async def collect_evidence(entity_id, entity_type, business_key, round_number, context):
    all_evidence = []
    entity = {"id": entity_id, "entity_type": entity_type, "label": context.get("primary", {}).get("label", "")}
    tasks = []
    if entity_type == "company":
        tasks.append(nominatim_collect(entity, context))
        tasks.append(overpass_collect(entity, context))
        if SCRAPIQ_DISPATCH_URL:
            tasks.append(scrapiq_collect(entity, context))
    elif entity_type == "contact":
        tasks.append(social_analyzer_collect(entity, context))
        if SCRAPIQ_DISPATCH_URL:
            tasks.append(scrapiq_collect(entity, context))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, list):
            all_evidence.extend(result)
        elif isinstance(result, Exception):
            log.warning(f"Collector error: {result}")
    if all_evidence:
        conn = await asyncpg.connect(ENRICHMENT_DB_DSN)
        try:
            for ev in all_evidence:
                await conn.execute("INSERT INTO raw_evidence (entity_id, collector_name, source_url, content_type, raw_content, source_weight, round_number, scrapiq_request_id, scrapiq_result_id, business_key) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10)", ev.entity_id, ev.collector_name, ev.source_url, ev.content_type, ev.raw_content, ev.source_weight, ev.round_number, ev.scrapiq_request_id, ev.scrapiq_result_id, ev.business_key or business_key)
        finally:
            await conn.close()
        log.info(f"Stored {len(all_evidence)} evidence rows for {entity_id}")
    return len(all_evidence)

async def extract_fields(entity_id, round_number, business_key):
    conn = await asyncpg.connect(ENRICHMENT_DB_DSN)
    try:
        evidence = await conn.fetch("SELECT id, collector_name, source_url, content_type, raw_content FROM raw_evidence WHERE entity_id=$1 ORDER BY scraped_at DESC LIMIT 40", entity_id)
        if not evidence:
            return 0
        entity_type = str(await conn.fetchval("SELECT entity_type FROM entities WHERE id=$1::uuid", entity_id) or "generic")
        entity_label = str(await conn.fetchval("SELECT label FROM entities WHERE id=$1::uuid", entity_id) or "")
        seeded_website = await conn.fetchval("SELECT attr_value FROM entity_attributes WHERE entity_id=$1::uuid AND attr_key='website' ORDER BY confidence DESC LIMIT 1", entity_id)
        context_parts = []
        size = 0
        for e in evidence:
            part = f"[{e['collector_name']}] {e['source_url'] or ''}\n{(e['raw_content'] or '')[:1200]}"
            size += len(part)
            if size > 12000:
                break
            context_parts.append(part)
        context = "\n\n---\n\n".join(context_parts)
    finally:
        await conn.close()
    if not context:
        return 0
    extracted = {}
    keys_str = ", ".join(COMPANY_EXTRACT_FIELDS)
    prompt = f"""Extract only these fields from evidence: {keys_str}.\nRules:\n- return facts only when supported by evidence\n- confidence 0.0-1.0\n- if value is missing, omit field\nReturn JSON: {{"fields":[{{"key":"...","value":"...","confidence":0.0,"source_collector":"..."}}]}}\nEvidence:\n{context}\n"""
    try:
        raw = await llm_complete("extractor", prompt, json_mode=True, max_tokens=1400)
        parsed = json.loads(raw)
        fields = parsed.get("fields", []) if isinstance(parsed, dict) else []
        for f in fields:
            key = str(f.get("key") or "").strip()
            if key not in COMPANY_EXTRACT_FIELDS:
                continue
            val = _normalize_field(key, f.get("value"))
            if val is None:
                continue
            conf = max(0.0, min(1.0, float(f.get("confidence") or 0.0)))
            src = str(f.get("source_collector") or "extractor").strip()
            conf = _calibrate_confidence(key, val, conf, src)
            prev = extracted.get((key, val))
            if prev is None or conf > prev[0]:
                extracted[(key, val)] = (conf, src)
    except Exception as e:
        log.warning(f"LLM extraction failed: {e}")
    heuristic = _heuristic_fields_from_evidence(list(evidence), entity_label, seeded_website)
    for key, (val, conf, src) in heuristic.items():
        id_key = (key, val)
        if id_key not in extracted or conf > extracted[id_key][0]:
            extracted[id_key] = (_calibrate_confidence(key, val, conf, src), src)
    inserted = 0
    conn = await asyncpg.connect(ENRICHMENT_DB_DSN)
    try:
        for (key, val), (conf, src) in extracted.items():
            result = await conn.execute("INSERT INTO entity_attributes (entity_id, attr_key, attr_value, confidence, round_number, extraction_method, source_collector, llm_alias_used) SELECT $1, $2, $3, $4, $5, $6, $7, $8 WHERE NOT EXISTS (SELECT 1 FROM entity_attributes WHERE entity_id=$1 AND attr_key=$2 AND attr_value=$3)", entity_id, key, val, conf, round_number, "llm_field_specific", src, "extractor")
            if "INSERT 0 1" in str(result):
                inserted += 1
    finally:
        await conn.close()
    log.info(f"Extracted {inserted} fields for {entity_id} in round {round_number}")
    return inserted

async def promote_trusted(entity_id):
    conn = await asyncpg.connect(ENRICHMENT_DB_DSN)
    try:
        result = await conn.execute("UPDATE entity_attributes ea SET is_trusted=true, updated_at=NOW() WHERE ea.entity_id=$1 AND ea.is_trusted=false AND ((SELECT COUNT(DISTINCT source_collector) FROM entity_attributes ea2 WHERE ea2.entity_id=ea.entity_id AND ea2.attr_key=ea.attr_key AND ea2.attr_value=ea.attr_value) >= 2 OR ea.confidence >= 0.95 OR (ea.attr_key='linkedin_company_url' AND ea.confidence >= 0.9) OR (ea.attr_key='linkedin_company_urls' AND ea.confidence >= 0.85))", entity_id)
        return int(str(result).split()[-1])
    finally:
        await conn.close()

async def evaluate_gap(entity_id, round_number):
    conn = await asyncpg.connect(ENRICHMENT_DB_DSN)
    try:
        row = await conn.fetchrow("WITH best AS (SELECT attr_key, MAX(confidence) AS best_conf, BOOL_OR(is_trusted) AS trusted FROM entity_attributes WHERE entity_id=$1::uuid GROUP BY attr_key) SELECT COALESCE(COUNT(*) FILTER (WHERE trusted), 0)::int AS trusted_fields, COALESCE(AVG(best_conf), 0.0)::float AS overall_confidence FROM best", entity_id)
        trusted_fields = int(row["trusted_fields"] if row else 0)
        overall_confidence = float(row["overall_confidence"] if row else 0.0)
        await conn.execute("UPDATE entities SET enrichment_round=GREATEST(enrichment_round, $2), overall_confidence=$3, updated_at=NOW() WHERE id=$1::uuid", entity_id, round_number, overall_confidence)
        should_continue = round_number < 3 and (trusted_fields < MIN_TRUSTED_FIELDS or overall_confidence < MIN_OVERALL_CONFIDENCE)
        return should_continue, trusted_fields, overall_confidence
    finally:
        await conn.close()

async def write_enriched_to_crm(entity_id, business_key):
    crm_dsn = BUSINESS_TO_CRM.get(business_key, CRM_INTEGRITASMRV_DSN)
    conn = await asyncpg.connect(ENRICHMENT_DB_DSN)
    try:
        entity = await conn.fetchrow("SELECT id, entity_type, label, source_system, external_ids FROM entities WHERE id=$1::uuid", entity_id)
        if not entity:
            return {"status": "skipped", "reason": "entity not found"}
        attrs = await conn.fetch("SELECT attr_key, attr_value, confidence FROM entity_attributes WHERE entity_id=$1::uuid AND is_trusted=true ORDER BY confidence DESC", entity_id)
    finally:
        await conn.close()
    if not attrs:
        return {"status": "skipped", "reason": "no trusted attributes"}
    crm_conn = await asyncpg.connect(crm_dsn)
    try:
        table = "nb_crm_customers" if entity["entity_type"] == "company" else "nb_crm_contacts"
        id_col = "hubspot_id"
        ext_ids = entity.get("external_ids") or {}
        if isinstance(ext_ids, str):
            try: ext_ids = json.loads(ext_ids)
            except: ext_ids = {}
        hs_id = ext_ids.get("hubspot_id") if isinstance(ext_ids, dict) else None
        if not hs_id:
            return {"status": "skipped", "reason": "no hubspot_id"}
        for attr in attrs:
            key = attr["attr_key"]
            val = attr["attr_value"]
            conf = float(attr["confidence"])
            try:
                await crm_conn.execute(f'UPDATE {table} SET enrichment_score = GREATEST(COALESCE(enrichment_score, 0), $3), "updatedAt" = NOW() WHERE {id_col} = $1 AND enrichment_score < $3', hs_id, val, conf if key == "overall_confidence" else 0)
            except: pass
        return {"status": "synced", "crm_fields": len(attrs)}
    finally:
        await crm_conn.close()

async def write_hubspot(entity_id):
    conn = await asyncpg.connect(ENRICHMENT_DB_DSN)
    try:
        entity = await conn.fetchrow("SELECT id::text AS id, entity_type, label, source_system, external_ids, enrichment_round, overall_confidence FROM entities WHERE id=$1::uuid", entity_id)
        if not entity:
            return {"status": "skipped", "reason": "entity not found"}
        attrs = await conn.fetch("SELECT attr_key, attr_value, attr_value_json, confidence, source_collector FROM entity_attributes WHERE entity_id=$1::uuid AND is_trusted=true ORDER BY confidence DESC", entity_id)
    finally:
        await conn.close()
    if not HS_SYNC_URL:
        return {"status": "skipped", "reason": "HUBSPOT_SYNC_URL not set"}
    trusted = {}
    for row in attrs:
        val = row["attr_value_json"] if row["attr_value_json"] is not None else row["attr_value"]
        trusted[row["attr_key"]] = {"value": val, "confidence": float(row["confidence"] or 0.0), "source_collector": row["source_collector"]}
    payload = {"entity": {"id": entity["id"], "entity_type": entity["entity_type"], "label": entity["label"], "source_system": entity["source_system"], "external_ids": entity["external_ids"] or {}, "enrichment_round": int(entity["enrichment_round"] or 0), "overall_confidence": float(entity["overall_confidence"] or 0.0)}, "trusted_attributes": trusted}
    headers = {}
    if HS_TOKEN:
        headers["Authorization"] = f"Bearer {HS_TOKEN}"
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(HS_SYNC_URL, json=payload, headers=headers)
            resp.raise_for_status()
        return {"status": "synced", "trusted_fields": len(trusted)}
    except Exception as e:
        return {"status": "failed", "error": str(e)[:200]}

# === HATCHET WORKFLOWS ===

hatchet = Hatchet(config=ClientConfig(server_url=os.environ.get("HATCHET_CLIENT_SERVER_URL", "http://hatchet-engine:8080")))

class EnrichmentInput(BaseModel):
    entity_id: str
    entity_type: str
    business_key: str
    source_system: str = "hubspot"
    hubspot_id: str = ""
    crm_name: str = "integritasmrv"
    crm_entity_id: str = ""
    name: str = ""
    email: str = ""

class EnrichmentOutput(BaseModel):
    status: str
    rounds: int = 0
    trusted_fields: int = 0
    overall_confidence: float = 0.0
    error: str = ""

enrich_wf = hatchet.workflow(name="enrichment-pipeline-v2", input_validator=EnrichmentInput, on_events=["enrichment-v2-request", "contact-enrichment-v2-request"])

@enrich_wf.task(name="run-enrichment", retries=2, timeout="20m")
async def run_enrichment(input: EnrichmentInput, ctx: Context) -> EnrichmentOutput:
    eid = input.entity_id
    bk = input.business_key
    entity_type = input.entity_type
    try:
        conn = await asyncpg.connect(ENRICHMENT_DB_DSN)
        try:
            await conn.execute("UPDATE entities SET enrichment_status='in_progress', updated_at=NOW() WHERE id=$1::uuid", eid)
        finally:
            await conn.close()
        current_round = 1
        total_trusted = 0
        total_confidence = 0.0
        while current_round <= 3:
            log.info(f"Round {current_round} for {eid} ({bk})")
            context = await build_context_bundle(eid, current_round, bk)
            if not context:
                log.warning(f"No context built for {eid}")
                break
            evidence_count = await collect_evidence(eid, entity_type, bk, current_round, context)
            log.info(f"Collected {evidence_count} evidence for {eid} round {current_round}")
            if evidence_count > 0:
                inserted = await extract_fields(eid, current_round, bk)
            else:
                inserted = 0
            trusted = await promote_trusted(eid)
            log.info(f"Promoted {trusted} trusted fields for {eid} round {current_round}")
            should_continue, total_trusted, total_confidence = await evaluate_gap(eid, current_round)
            log.info(f"Gap eval for {eid}: trusted={total_trusted}, conf={total_confidence:.3f}, continue={should_continue}")
            if not should_continue:
                break
            current_round += 1
        crm_result = await write_enriched_to_crm(eid, bk)
        log.info(f"CRM writeback for {eid}: {crm_result}")
        hs_result = await write_hubspot(eid)
        log.info(f"HubSpot sync for {eid}: {hs_result}")
        conn = await asyncpg.connect(ENRICHMENT_DB_DSN)
        try:
            await conn.execute("UPDATE entities SET enrichment_status='done', updated_at=NOW() WHERE id=$1::uuid", eid)
        finally:
            await conn.close()
        return EnrichmentOutput(status="completed", rounds=current_round, trusted_fields=total_trusted, overall_confidence=total_confidence)
    except Exception as e:
        log.exception(f"Enrichment failed for {eid}")
        try:
            conn = await asyncpg.connect(ENRICHMENT_DB_DSN)
            try:
                await conn.execute("UPDATE entities SET enrichment_status='failed', updated_at=NOW() WHERE id=$1::uuid", eid)
            finally:
                await conn.close()
        except: pass
        return EnrichmentOutput(status="failed", error=str(e)[:200])

class HubSpotInput(BaseModel):
    email: str
    firstname: str = ""
    lastname: str = ""
    company: str = ""

class HubSpotOutput(BaseModel):
    hubspot_id: str = ""
    status: str

hs_wf = hatchet.workflow(name="hubspot-sync-v2", input_validator=HubSpotInput, on_events=["hubspot-sync"])

@hs_wf.task(name="sync-contact-v2", retries=1, timeout="30s")
async def sync_contact(input: HubSpotInput, ctx: Context) -> HubSpotOutput:
    if not HS_TOKEN:
        return HubSpotOutput(status="skipped_no_token")
    headers = {"Authorization": f"Bearer {HS_TOKEN}", "Content-Type": "application/json"}
    props = {"email": input.email}
    for k in ["firstname", "lastname", "company"]:
        v = getattr(input, k, None)
        if v:
            props[k] = v
    async with httpx.AsyncClient(timeout=httpx.Timeout(15)) as c:
        r = await c.post("https://api.hubapi.com/crm/v3/objects/contacts", headers=headers, json={"properties": props})
        if r.status_code in (200, 201):
            return HubSpotOutput(hubspot_id=r.json().get("id"), status="created")
        return HubSpotOutput(status=f"error:{r.status_code}")

worker = hatchet.worker("integritas-worker-v2", workflows=[enrich_wf, hs_wf])

if __name__ == "__main__":
    log.info("Starting Hatchet worker v2...")
    worker.start()

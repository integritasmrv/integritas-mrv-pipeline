import os, json, re, logging, asyncio, httpx, asyncpg
from hatchet_sdk import Hatchet, Context
from pydantic import BaseModel
from urllib.parse import urlparse
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [worker-v2] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

T = os.environ.get("HATCHET_CLIENT_TOKEN", "eyJhbGciOiJFUzI1NiIsImtpZCI6ImZ5cTd3QSJ9.eyJhdWQiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCJleHAiOjE3ODQzNjU5MTUsImdycGNfYnJvYWRjYXN0X2FkZHJlc3MiOiJoYXRjaGV0LWVuZ2luZTo3MDcwIiwiaWF0IjoxNzc2NTg5OTE1LCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwODAiLCJzZXJ2ZXJfdXJsIjoiaHR0cDovL2xvY2FsaG9zdDo4MDgwIiwic3ViIjoiNzA3ZDA4NTUtODBhYi00ZTFmLWExNTYtZjFjNDU0NmNiZjUyIiwidG9rZW5faWQiOiJiMDc5MTc4Zi02OGQ4LTRhMzQtYThlYy1kNzQ2YjE4OGZkMzcifQ.FKSllU33io3P3Dl4bsWvdxPQzHCnXmKpU3PK_vx27DhQvaYjMmFzB91UT2Jw82mvcolXtNBK9tSNP_KcabHCKw")
os.environ["HATCHET_CLIENT_TOKEN"] = T
os.environ["HATCHET_CLIENT_HOST_PORT"] = os.environ.get("HATCHET_CLIENT_HOST_PORT", "127.0.0.1:7070")
os.environ["HATCHET_CLIENT_TLS_STRATEGY"] = "none"
hatchet = Hatchet()

ENRICHMENT_DB = {
    "host": os.environ.get("ENRICHMENT_DB_HOST", "10.0.0.2"),
    "port": int(os.environ.get("ENRICHMENT_DB_PORT", "5432")),
    "user": os.environ.get("ENRICHMENT_DB_USER", "hatchet_user"),
    "password": os.environ.get("ENRICHMENT_DB_PASS", "Hatch3tDB2026!"),
    "database": os.environ.get("ENRICHMENT_DB_NAME", "enrichment_ev_batteries"),
}

CRM_DBS = {
    "integritasmrv": {
        "host": "127.0.0.1", "port": 15432, "user": "integritasmrv_crm_user",
        "password": "oYxxPKRfAHAD263VSDcKmljKY0vInx2QTl6PooKoqmmiDops", "database": "integritasmrv_crm",
    },
    "poweriq": {
        "host": "127.0.0.1", "port": 15433, "user": "poweriq_crm_user",
        "password": "P0w3r1Q_CRM_S3cur3_P@ss_2026", "database": "poweriq_crm",
    },
}

HS_TOKEN = os.environ.get("HUBSPOT_API_TOKEN", "")
HUBSPOT_SYNC_URL = os.environ.get("HUBSPOT_SYNC_URL", "")
LITELLM = os.environ.get("LITELLM_PROXY_URL", "http://10.0.4.8:4000")
LITELLM_KEY = os.environ.get("LITELLM_PROXY_KEY", "")
NOMINATIM_URL = os.environ.get("NOMINATIM_SEARCH_URL", "http://217.76.59.199:8180/search")
OVERPASS_URL = os.environ.get("OVERPASS_URL", "https://overpass-api.de/api/interpreter")
SCRAPIQ_DISPATCH_URL = os.environ.get("SCRAPIQ_DISPATCH_URL", "")
SOCIAL_ANALYZER_URL = os.environ.get("SOCIAL_ANALYZER_URL", "http://intelligence-social-analyzer:9005")
MIN_TRUSTED = 6
MIN_CONF = 0.75
MAX_ROUNDS = 3

BUSINESS_TO_SCRAPIQ = {
    "integritasmrv": "integritas", "poweriq": "pwriq",
    "airbnb": "airbnb", "ev-batteries": "ev-batteries", "private": "private",
}

HUBSPOT_PROPERTY_MAP = {
    'legal_name': 'legal_entity_name', 'trade_name': 'name', 'website': 'website',
    'email': 'email', 'phone': 'phone', 'street': 'address', 'city': 'city',
    'postal_code': 'postal_code', 'country': 'country', 'linkedin_url': 'linkedin_url',
    'description': 'description', 'employee_count': 'numberofemployees',
    'first_name': 'firstname', 'last_name': 'lastname', 'job_title': 'jobtitle',
    'company_name': 'company',
}

COMPANY_EXTRACT_FIELDS = [
    "legal_name", "trade_name", "vat_number", "registration_number", "website",
    "email", "phone", "street", "city", "postal_code", "country", "industry_code",
    "linkedin_url", "description", "employee_count", "founded_year", "technologies",
]

DIRECTORY_DOMAINS = {"linkedin.com", "crunchbase.com", "pitchbook.com", "wikipedia.org",
    "facebook.com", "x.com", "twitter.com", "instagram.com", "youtube.com"}

LINKEDIN_BRANCH_HINTS = {"deutschland", "germany", "france", "italy", "spain", "poland",
    "uk", "usa", "us", "canada", "australia", "india", "japan", "china",
    "belgium", "netherlands", "mexico", "brazil"}


# === LITE LLM ===
async def llm_complete(alias: str, prompt: str, json_mode: bool = False,
                      max_tokens: int = 2048, temperature: float = 0.1) -> str:
    payload = {
        "model": alias,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    if json_mode:
        payload["response_format"] = {"type": "json_object"}
    headers = {"Content-Type": "application/json"}
    if LITELLM_KEY:
        headers["Authorization"] = f"Bearer {LITELLM_KEY}"
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(f"{LITELLM}/chat/completions", json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
    return ((data.get("choices") or [{}])[0].get("message") or {}).get("content", "")


async def llm_embed(text: str) -> list[float]:
    headers = {"Content-Type": "application/json"}
    if LITELLM_KEY:
        headers["Authorization"] = f"Bearer {LITELLM_KEY}"
    payload = {"model": "embedder", "input": text}
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(f"{LITELLM}/embeddings", json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
    return (data.get("data") or [{}])[0].get("embedding") or []


async def lightrag_insert(text: str, business_key: str = "integritasmrv") -> dict:
    headers = {"Content-Type": "application/json"}
    if LITELLM_KEY:
        headers["Authorization"] = f"Bearer {LITELLM_KEY}"
    payload = {"text": text, "workspace": business_key}
    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post("http://intelligence-lightrag:9621/insert", json=payload, headers=headers)
        r.raise_for_status()
        return r.json()


async def qdrant_upsert(collection: str, point_id: str, vector: list,
                        payload: dict, business_key: str = "integritasmrv") -> None:
    headers = {"Content-Type": "application/json"}
    if LITELLM_KEY:
        headers["Authorization"] = f"Bearer {LITELLM_KEY}"
    url = f"http://intelligence-qdrant:6333/collections/{collection}/points"
    point = {"id": point_id, "vector": vector, "payload": {**payload, "business_key": business_key}}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.put(url, json={"points": [point]}, headers=headers)
        if r.status_code not in (200, 201):
            log.warning("Qdrant upsert failed: %s %s", r.status_code, r.text[:200])


# === DATABASE HELPERS ===
async def get_enrichment_conn():
    return await asyncpg.connect(
        host=ENRICHMENT_DB["host"], port=ENRICHMENT_DB["port"],
        user=ENRICHMENT_DB["user"], password=ENRICHMENT_DB["password"],
        database=ENRICHMENT_DB["database"], timeout=30,
    )

async def get_crm_conn(crm_name: str = "integritasmrv"):
    cfg = CRM_DBS.get(crm_name, CRM_DBS["integritasmrv"])
    return await asyncpg.connect(
        host=cfg["host"], port=cfg["port"],
        user=cfg["user"], password=cfg["password"],
        database=cfg["database"], timeout=30,
    )


# === NORMALIZATION HELPERS ===
def _normalize_field_value(key: str, value: object) -> str | None:
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
        if len(m) == 4:
            return m
        return None
    if key in ("lat", "lng"):
        try:
            return str(float(v))
        except Exception:
            return None
    if key == "country_code":
        cc = re.sub(r"[^a-zA-Z]", "", v).lower()
        if len(cc) == 2:
            return cc
        return None
    if key == "phone":
        return re.sub(r"\s+", " ", v)[:120]
    return v[:4000]


def _calibrate_confidence(key: str, value: str, confidence: float, source: str) -> float:
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
    elif key == "linkedin_company_urls":
        conf = max(conf, 0.9)
    elif key == "company_description":
        conf = min(conf, 0.78)
    elif key in ("lat", "lng"):
        if "nominatim" in src:
            conf = max(conf, 0.95)
    elif key in ("address", "city", "postcode", "country", "country_code"):
        if "nominatim" in src:
            conf = max(conf, 0.9)
    elif key in ("phone", "website"):
        if "overpass" in src:
            conf = max(conf, 0.82)
    elif key == "website":
        conf = max(conf, 0.8)
    return max(0.0, min(1.0, conf))


def _normalize_linkedin_company_url(url: str) -> str | None:
    raw = (url or "").strip()
    if not raw:
        return None
    try:
        parsed = urlparse(raw)
    except Exception:
        return None
    host = (parsed.netloc or "").lower().replace("www.", "")
    if "linkedin.com" not in host:
        return None
    parts = [p for p in (parsed.path or "").split("/") if p]
    if len(parts) < 2 or parts[0].lower() != "company":
        return None
    slug = parts[1].strip()
    if not slug:
        return None
    return f"https://www.linkedin.com/company/{slug}"


def _extract_linkedin_company_urls(text: str) -> list[str]:
    if not text:
        return []
    pattern = re.compile(
        r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/company/[A-Za-z0-9\-_%/]+",
        re.IGNORECASE,
    )
    out: list[str] = []
    for match in pattern.findall(text):
        cleaned = match.strip().rstrip(".,;:)\"')")
        normalized = _normalize_linkedin_company_url(cleaned)
        if normalized and normalized not in out:
            out.append(normalized)
    return out


def _entity_tokens(entity_name: str) -> list[str]:
    return [t for t in re.findall(r"[a-z0-9]+", (entity_name or "").lower()) if len(t) >= 3]


def _company_slug_from_entity_label(entity_label: str) -> str:
    toks = _entity_tokens(entity_label)
    if not toks:
        return ""
    return "-".join(toks)


def _slug_branch_penalty(slug: str, entity_label: str) -> int:
    lower_slug = (slug or "").lower()
    lower_label = (entity_label or "").lower()
    for hint in LINKEDIN_BRANCH_HINTS:
        if hint in lower_slug and hint not in lower_label:
            return 8
    return 0


def _domain_is_directory(domain: str) -> bool:
    d = (domain or "").lower().replace("www.", "")
    return any(d == item or d.endswith("." + item) for item in DIRECTORY_DOMAINS)


def _compact_summary(text: str, max_chars: int = 1000) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "")).strip()
    return cleaned[:max_chars] if len(cleaned) <= max_chars else cleaned[:max_chars].rstrip()


def _build_evidence_context(evidence_rows: list, max_chars: int = 12000) -> str:
    chunks: list[str] = []
    size = 0
    for e in evidence_rows:
        part = (f"[{e.get('collector_name', '')}] {e.get('source_url', '') or ''}\n"
                f"{(e.get('raw_content', '') or '')[:1200]}")
        size += len(part)
        if size > max_chars:
            break
        chunks.append(part)
    return "\n\n---\n\n".join(chunks)


def _map_to_hubspot_properties(entity_type: str, attributes: dict) -> dict:
    p = {}
    for k, v in attributes.items():
        val = v.get('value')
        if val is None:
            continue
        mapped = HUBSPOT_PROPERTY_MAP.get(k, k)
        p[mapped] = str(val)
    confs = [v.get('confidence', 0) for v in attributes.values()]
    if confs:
        p['enrichment_confidence'] = str(max(confs))
    return p

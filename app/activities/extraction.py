from dotenv import load_dotenv
load_dotenv("/opt/enrichiq/.env")

import json
import os
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

import asyncpg
import httpx

HUBSPOT_PROPERTY_MAP = {'legal_name': 'legal_entity_name', 'trade_name': 'name', 'website': 'website', 'email': 'email', 'phone': 'phone', 'street': 'address', 'city': 'city', 'postal_code': 'postal_code', 'country': 'country', 'linkedin_url': 'linkedin_url', 'description': 'description', 'employee_count': 'numberofemployees', 'first_name': 'firstname', 'last_name': 'lastname', 'job_title': 'jobtitle', 'company_name': 'company'}

def _map_to_hubspot_properties(entity_type, attributes):
    p = {}
    for k, v in attributes.items():
        val = v.get('value')
        if val is None: continue
        p[HUBSPOT_PROPERTY_MAP.get(k, k)] = str(val)
    confs = [v.get('confidence', 0) for v in attributes.values()]
    if confs: p['enrichment_confidence'] = str(max(confs))
    return p

from app.llm import llm_complete, llm_embed, lightrag_insert, qdrant_upsert

ENRICHMENT_DB_URL = os.environ.get(
    "DATABASE_URL", os.environ.get("ENRICHMENT_DB_DSN", "")
)
MIN_TRUSTED_FIELDS = int(os.environ.get("ENRICHMENT_MIN_TRUSTED_FIELDS", "6"))
MIN_OVERALL_CONFIDENCE = float(
    os.environ.get("ENRICHMENT_MIN_OVERALL_CONFIDENCE", "0.75")
)
HUBSPOT_SYNC_URL = os.environ.get("HUBSPOT_SYNC_URL", "")
HUBSPOT_SYNC_TOKEN = os.environ.get("HUBSPOT_SYNC_TOKEN", "")

COMPANY_EXTRACT_FIELDS = [
    "legal_name",
    "trade_name",
    "vat_number",
    "registration_number",
    "website",
    "email",
    "phone",
    "street",
    "city",
    "postal_code",
    "country",
    "industry_code",
    "linkedin_url",
    "description",
    "employee_count",
    "founded_year",
    "technologies",
]

FIELD_EXTRACTION_SPECS = [
    {
        "name": "company_full_profile",
        "keys": COMPANY_EXTRACT_FIELDS,
    }
]

SUMMARY_PROMPTS_BY_ENTITY: dict[str, str] = {
    "company": "Summarize this company using evidence only.",
    "contact": "Summarize this contact person using evidence only.",
    "product": "Summarize this product using evidence only.",
    "generic": "Summarize this entity using evidence only.",
}

DIRECTORY_DOMAINS = {
    "linkedin.com",
    "crunchbase.com",
    "pitchbook.com",
    "wikipedia.org",
    "facebook.com",
    "x.com",
    "twitter.com",
    "instagram.com",
    "youtube.com",
}

LINKEDIN_BRANCH_HINTS = {
    "deutschland",
    "germany",
    "france",
    "italy",
    "spain",
    "poland",
    "uk",
    "usa",
    "us",
    "canada",
    "australia",
    "india",
    "japan",
    "china",
    "belgium",
    "netherlands",
    "mexico",
    "brazil",
}


def _normalize_field_value(key: str, value: object) -> str | None:
    if value in (None, ""):
        return None
    v = str(value).strip()
    if not v:
        return None
    if key in ("official_website", "linkedin_company_url") and not v.startswith(
        ("http://", "https://")
    ):
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
        compact = re.sub(r"\s+", " ", v)
        return compact[:120]
    return v[:4000]


def _calibrate_confidence(
    key: str, value: str, confidence: float, source: str
) -> float:
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


def _build_evidence_context(
    evidence_rows: list[asyncpg.Record], max_chars: int = 12000
) -> str:
    chunks: list[str] = []
    size = 0
    for e in evidence_rows:
        part = (
            f"[{e['collector_name']}] {e['source_url'] or ''}\n"
            f"{(e['raw_content'] or '')[:1200]}"
        )
        size += len(part)
        if size > max_chars:
            break
        chunks.append(part)
    return "\n\n---\n\n".join(chunks)


def _entity_tokens(entity_name: str) -> list[str]:
    return [
        t for t in re.findall(r"[a-z0-9]+", (entity_name or "").lower()) if len(t) >= 3
    ]


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
        cleaned = match.strip().rstrip(".,;:)\"'")
        normalized = _normalize_linkedin_company_url(cleaned)
        if normalized and normalized not in out:
            out.append(normalized)
    return out


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
    for item in DIRECTORY_DOMAINS:
        if d == item or d.endswith("." + item):
            return True
    return False


def _compact_summary(text: str, max_chars: int = 1000) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "")).strip()
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[:max_chars].rstrip()


def _entity_summary_prompt(entity_type: str, context: str) -> str:
    base = SUMMARY_PROMPTS_BY_ENTITY.get(
        entity_type, SUMMARY_PROMPTS_BY_ENTITY["generic"]
    )
    return (
        f"{base} Return plain text only, max 1000 characters, no markdown, no bullet list. "
        "Include only facts supported by the evidence.\n\n"
        f"Evidence:\n{context[:9000]}"
    )


def _evidence_summary_prompt(
    entity_type: str, summary_kind: str, content: str, source_url: str
) -> str:
    return (
        f"Create a plain-text summary for one {summary_kind} related to a {entity_type}. "
        "Maximum 1000 characters. No markdown. Facts only from provided content.\n\n"
        f"Source URL: {source_url}\n"
        f"Content:\n{content[:7000]}"
    )


async def _ensure_summary_tables(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS entity_summaries (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            entity_id UUID NOT NULL,
            entity_type TEXT NOT NULL,
            round_number INTEGER NOT NULL,
            summary_text TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (entity_id, round_number)
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_evidence_summaries (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            raw_evidence_id UUID NOT NULL,
            entity_id UUID NOT NULL,
            summary_kind TEXT NOT NULL,
            round_number INTEGER NOT NULL,
            summary_text TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (raw_evidence_id, round_number)
        )
        """
    )


async def _create_standard_summaries(
    conn: asyncpg.Connection,
    *,
    entity_id: str,
    entity_type: str,
    round_number: int,
    evidence_rows: list[asyncpg.Record],
    context: str,
    business_key: str = "integritasmrv",
) -> None:
    await _ensure_summary_tables(conn)

    try:
        entity_summary = await llm_complete(
            "summarizer",
            _entity_summary_prompt(entity_type, context),
            max_tokens=450,
        )
    except Exception:
        entity_summary = context
    entity_summary = _compact_summary(entity_summary, max_chars=1000)
    if entity_summary:
        await conn.execute(
            """
            INSERT INTO entity_summaries (entity_id, entity_type, round_number, summary_text)
            VALUES ($1::uuid, $2, $3, $4)
            ON CONFLICT (entity_id, round_number)
            DO UPDATE SET
                entity_type = EXCLUDED.entity_type,
                summary_text = EXCLUDED.summary_text,
                updated_at = NOW()
            """,
            entity_id,
            entity_type,
            round_number,
            entity_summary,
        )

        try:
            await lightrag_insert(entity_summary, business_key)
            entity_vector = await llm_embed(entity_summary)
            if entity_vector:
                await qdrant_upsert("intelligence_data",entity_id,entity_vector,{"entity_type":entity_type,"round_number":round_number,"kind":"entity_summary"},business_key)
        except Exception:
            pass

    for ev in evidence_rows:
        row = dict(ev)
        ev_id = str(row.get("id") or "").strip()
        if not ev_id:
            continue
        content = str(row.get("raw_content") or "")
        if not content:
            continue
        content_type = str(row.get("content_type") or "").lower()
        source_url = str(row.get("source_url") or "")
        summary_kind = (
            "document"
            if any(k in content_type for k in ["pdf", "doc", "ppt", "xls"])
            else "page"
        )
        try:
            raw_summary = await llm_complete(
                "summarizer",
                _evidence_summary_prompt(
                    entity_type, summary_kind, content, source_url
                ),
                max_tokens=450,
            )
        except Exception:
            raw_summary = content
        summary_text = _compact_summary(raw_summary, max_chars=1000)
        if not summary_text:
            continue
        await conn.execute(
            """
            INSERT INTO raw_evidence_summaries
              (raw_evidence_id, entity_id, summary_kind, round_number, summary_text)
            VALUES ($1::uuid, $2::uuid, $3, $4, $5)
            ON CONFLICT (raw_evidence_id, round_number)
            DO UPDATE SET
                summary_kind = EXCLUDED.summary_kind,
                summary_text = EXCLUDED.summary_text,
                updated_at = NOW()
            """,
            ev_id,
            entity_id,
            summary_kind,
            round_number,
            summary_text,
        )

        try:
            await lightrag_insert(summary_text, business_key)
            ev_vector = await llm_embed(summary_text)
            if ev_vector:
                await qdrant_upsert("intelligence_data",ev_id,ev_vector,{"entity_id":entity_id,"summary_kind":summary_kind,"round_number":round_number,"kind":"evidence_summary"},business_key)
        except Exception:
            pass


def _heuristic_fields_from_evidence(
    evidence_rows: list[asyncpg.Record],
    entity_label: str,
    seeded_website: str | None,
) -> dict[tuple[str, str], tuple[float, str]]:
    out: dict[tuple[str, str], tuple[float, str]] = {}
    domains: dict[str, int] = {}
    linkedin_sources: dict[str, float] = {}
    entity_tokens = _entity_tokens(entity_label)
    entity_slug = _company_slug_from_entity_label(entity_label)
    seeded_domain = (
        urlparse((seeded_website or "")).netloc.lower().replace("www.", "")
        if seeded_website
        else ""
    )

    for e in evidence_rows:
        src_url = str(e.get("source_url") or "").strip()
        if not src_url:
            continue
        domain = (urlparse(src_url).netloc or "").lower().replace("www.", "")
        if not domain:
            continue
        domains[domain] = domains.get(domain, 0) + 1
        content = str(e.get("raw_content") or "")

        candidates: list[str] = []
        normalized_linkedin = _normalize_linkedin_company_url(src_url)
        if normalized_linkedin:
            candidates.append(normalized_linkedin)
        for c in _extract_linkedin_company_urls(content):
            if c not in candidates:
                candidates.append(c)

        for candidate in candidates:
            score = linkedin_sources.get(candidate, 0.0)
            score += 2.0
            lowered = candidate.lower()
            if any(tok in lowered for tok in entity_tokens):
                score += 2.0
            if seeded_domain and domain == seeded_domain:
                score += 7.0
            linkedin_sources[candidate] = score

    if linkedin_sources:
        scored: list[tuple[float, str]] = []
        linkedin_urls = list(linkedin_sources.keys())
        for u in linkedin_urls:
            lowered = u.lower()
            score = linkedin_sources.get(u, 0.0) + 10.0
            if "/life" not in lowered and "/jobs" not in lowered:
                score += 5
            if any(tok in lowered for tok in entity_tokens):
                score += 6
            if "/group" in lowered or "/holding" in lowered:
                score += 2
            slug = lowered.split("/company/")[-1].split("/")[0]
            if entity_slug and slug == entity_slug:
                score += 14
            score -= _slug_branch_penalty(slug, entity_label)
            scored.append((score, u))
        scored.sort(key=lambda x: x[0], reverse=True)
        primary = scored[0][1]
        out[("linkedin_company_url", primary)] = (0.95, "heuristic_linkedin_primary")
        out[("linkedin_company_urls", json.dumps(linkedin_urls))] = (
            0.9,
            "heuristic_linkedin_set",
        )

    if seeded_website and seeded_website.startswith(("http://", "https://")):
        out[("official_website", seeded_website)] = (0.97, "heuristic_seed_website")

    if domains and not any(key == "official_website" for (key, _v) in out.keys()):
        candidates: list[tuple[float, str]] = []
        for dom, freq in domains.items():
            if _domain_is_directory(dom):
                continue
            score = float(freq)
            if any(tok in dom for tok in entity_tokens):
                score += 2.5
            candidates.append((score, dom))
        if candidates:
            candidates.sort(key=lambda x: x[0], reverse=True)
            best_domain = candidates[0][1]
            out[("official_website", f"https://{best_domain}")] = (
                0.92,
                "heuristic_domain",
            )

    for e in evidence_rows:
        text = str(e.get("raw_content") or "")[:1200]
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        for ln in lines:
            if ln.startswith(("http://", "https://")):
                continue
            if len(ln) < 40:
                continue
            if re.search(r"^[\-*#]+\s*$", ln):
                continue
            out[("company_description", ln[:600])] = (0.62, "heuristic_text")
            break
        if any(key == "company_description" for (key, _v) in out.keys()):
            break

    for e in evidence_rows:
        collector = str(e.get("collector_name") or "").lower()
        raw = str(e.get("raw_content") or "")
        if not raw:
            continue
        if "nominatim" in collector:
            try:
                payload = json.loads(raw)
                lat = _normalize_field_value("lat", payload.get("lat"))
                lng = _normalize_field_value("lng", payload.get("lng"))
                addr = payload.get("address") or {}
                road = str(addr.get("road") or "").strip()
                house = str(addr.get("house_number") or "").strip()
                full_addr = (road + " " + house).strip()
                city = _normalize_field_value("city", addr.get("city"))
                postcode = _normalize_field_value("postcode", addr.get("postcode"))
                country = _normalize_field_value("country", addr.get("country"))
                cc = _normalize_field_value("country_code", addr.get("country_code"))
                if lat:
                    out[("lat", lat)] = (0.97, "nominatim_heuristic")
                if lng:
                    out[("lng", lng)] = (0.97, "nominatim_heuristic")
                if full_addr:
                    out[("address", full_addr)] = (0.92, "nominatim_heuristic")
                if city:
                    out[("city", city)] = (0.93, "nominatim_heuristic")
                if postcode:
                    out[("postcode", postcode)] = (0.9, "nominatim_heuristic")
                if country:
                    out[("country", country)] = (0.92, "nominatim_heuristic")
                if cc:
                    out[("country_code", cc)] = (0.96, "nominatim_heuristic")
            except Exception:
                pass
        if "overpass" in collector:
            try:
                payload = json.loads(raw)
                rows = payload.get("results") if isinstance(payload, dict) else None
                if not isinstance(rows, list):
                    rows = []
                for row in rows[:5]:
                    phone = _normalize_field_value("phone", row.get("phone"))
                    website = _normalize_field_value("website", row.get("website"))
                    if phone:
                        out[("phone", phone)] = (0.85, "overpass_heuristic")
                    if website:
                        out[("website", website)] = (0.84, "overpass_heuristic")
            except Exception:
                pass
    return out


async def extract_fields_llm(entity_id: str, round_number: int, business_key: str = "integritasmrv") -> int:
    if not ENRICHMENT_DB_URL:
        return 0
    conn = await asyncpg.connect(ENRICHMENT_DB_URL)
    try:
        evidence = await conn.fetch(
            """
            SELECT id, collector_name, source_url, content_type, raw_content
            FROM raw_evidence
            WHERE entity_id=$1
            ORDER BY scraped_at DESC
            LIMIT 40
            """,
            entity_id,
        )
        entity_type = str(
            (
                await conn.fetchval(
                    "SELECT entity_type FROM entities WHERE id=$1::uuid", entity_id
                )
            )
            or "generic"
        )
        entity_label = str(
            (
                await conn.fetchval(
                    "SELECT label FROM entities WHERE id=$1::uuid", entity_id
                )
            )
            or ""
        )
        seeded_website = await conn.fetchval(
            """
            SELECT attr_value
            FROM entity_attributes
            WHERE entity_id=$1::uuid
              AND attr_key='website'
            ORDER BY confidence DESC, updated_at DESC
            LIMIT 1
            """,
            entity_id,
        )
        context = _build_evidence_context(evidence)
        if not context:
            return 0

        await _create_standard_summaries(
            conn,
            entity_id=entity_id,
            entity_type=entity_type,
            round_number=round_number,
            evidence_rows=evidence,
            context=context,
            business_key=business_key,
        )

        extracted: dict[tuple[str, str], tuple[float, str]] = {}
        for spec in FIELD_EXTRACTION_SPECS:
            keys = ", ".join(spec["keys"])
            prompt = f"""Extract only these fields from evidence: {keys}.
Rules:
- return facts only when supported by evidence
- confidence 0.0-1.0
- if value is missing, omit field
Return JSON: {{"fields":[{{"key":"...","value":"...","confidence":0.0,"source_collector":"...","evidence_snippet":"..."}}]}}
Evidence:
{context}
"""
            try:
                raw = await llm_complete(
                    "extractor", prompt, json_mode=True, max_tokens=1400
                )
                parsed = json.loads(raw)
            except Exception:
                continue
            fields = parsed.get("fields", []) if isinstance(parsed, dict) else []
            for f in fields:
                key = str(f.get("key") or "").strip()
                if key not in spec["keys"]:
                    continue
                val = _normalize_field_value(key, f.get("value"))
                if val is None:
                    continue
                conf = max(0.0, min(1.0, float(f.get("confidence") or 0.0)))
                src = (
                    str(f.get("source_collector") or spec["name"]).strip()
                    or spec["name"]
                )
                conf = _calibrate_confidence(key, val, conf, src)
                id_key = (key, val)
                prev = extracted.get(id_key)
                if (prev is None) or (conf > prev[0]):
                    extracted[id_key] = (conf, src)

        for id_key, data in _heuristic_fields_from_evidence(
            evidence, entity_label=entity_label, seeded_website=seeded_website
        ).items():
            if id_key not in extracted:
                k, v = id_key
                c, s = data
                extracted[id_key] = (_calibrate_confidence(k, v, c, s), s)

        inserted = 0
        BLOCKED_ATTR_KEYS = {
            "opening_hours",
            "business_hours",
            "hours_of_operation",
        }
        for (key, val), (conf, src) in extracted.items():
            if key in BLOCKED_ATTR_KEYS:
                continue
            if key == "linkedin_company_urls":
                result = await conn.execute(
                    """
                    INSERT INTO entity_attributes
                      (entity_id, attr_key, attr_value, attr_value_json, confidence, round_number, extraction_method, source_collector, llm_alias_used)
                    SELECT $1,$2,NULL,$3::jsonb,$4,$5,'llm_field_specific',$6,'extractor'
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM entity_attributes
                        WHERE entity_id=$1
                          AND attr_key=$2
                    )
                    """,
                    entity_id,
                    key,
                    val,
                    conf,
                    round_number,
                    src,
                )
            else:
                result = await conn.execute(
                    """
                    INSERT INTO entity_attributes
                      (entity_id, attr_key, attr_value, confidence, round_number, extraction_method, source_collector, llm_alias_used)
                    SELECT $1,$2,$3,$4,$5,'llm_field_specific',$6,'extractor'
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM entity_attributes
                        WHERE entity_id=$1
                          AND attr_key=$2
                          AND attr_value=$3
                    )
                    """,
                    entity_id,
                    key,
                    val,
                    conf,
                    round_number,
                    src,
                )
            if str(result).endswith("1"):
                inserted += 1
        return inserted
    except Exception:
        return 0
    finally:
        await conn.close()


async def promote_trusted_fields(entity_id: str, business_key: str | None = None) -> int:
    if not ENRICHMENT_DB_URL:
        return 0
    conn = await asyncpg.connect(ENRICHMENT_DB_URL)
    try:
        if business_key is None:
            business_key = await conn.fetchval(
                "SELECT source_system FROM entities WHERE id=$1::uuid", entity_id
            ) or "integritasmrv"
        result = await conn.execute(
            """
            UPDATE entity_attributes ea
            SET is_trusted = true, updated_at=NOW()
            WHERE ea.entity_id = $1
              AND ea.is_trusted = false
              AND (
                (SELECT COUNT(DISTINCT source_collector)
                 FROM entity_attributes ea2
                 WHERE ea2.entity_id = ea.entity_id
                   AND ea2.attr_key = ea.attr_key
                   AND ea2.attr_value = ea.attr_value) >= 2
                OR ea.confidence >= 0.95
                OR (ea.attr_key = 'linkedin_company_url' AND ea.confidence >= 0.9)
                OR (ea.attr_key = 'linkedin_company_urls' AND ea.confidence >= 0.85)
              )
            """,
            entity_id,
        )
        return int(result.split()[-1])
    finally:
        await conn.close()


async def evaluate_confidence_gap(entity_id: str, round_number: int, business_key: str | None = None) -> dict:
    if not ENRICHMENT_DB_URL:
        return {
            "should_continue": False,
            "round": round_number,
            "trusted_fields": 0,
            "overall_confidence": 0.0,
            "min_trusted_fields": MIN_TRUSTED_FIELDS,
            "min_overall_confidence": MIN_OVERALL_CONFIDENCE,
        }

    conn = await asyncpg.connect(ENRICHMENT_DB_URL)
    try:
        row = await conn.fetchrow(
            """
            WITH best AS (
                SELECT attr_key,
                       MAX(confidence) AS best_conf,
                       BOOL_OR(is_trusted) AS trusted
                FROM entity_attributes
                WHERE entity_id = $1::uuid
                GROUP BY attr_key
            )
            SELECT
                COALESCE(COUNT(*) FILTER (WHERE trusted), 0)::int AS trusted_fields,
                COALESCE(AVG(best_conf), 0.0)::float AS overall_confidence
            FROM best
            """,
            entity_id,
        )
        trusted_fields = int(row["trusted_fields"] if row else 0)
        overall_confidence = float(row["overall_confidence"] if row else 0.0)

        await conn.execute(
            """
            UPDATE entities
            SET enrichment_round = GREATEST(enrichment_round, $2),
                overall_confidence = $3,
                updated_at = NOW()
            WHERE id = $1::uuid
            """,
            entity_id,
            round_number,
            overall_confidence,
        )

        should_continue = round_number < 3 and (
            trusted_fields < MIN_TRUSTED_FIELDS
            or overall_confidence < MIN_OVERALL_CONFIDENCE
        )

        return {
            "should_continue": should_continue,
            "round": round_number,
            "trusted_fields": trusted_fields,
            "overall_confidence": overall_confidence,
            "min_trusted_fields": MIN_TRUSTED_FIELDS,
            "min_overall_confidence": MIN_OVERALL_CONFIDENCE,
        }
    finally:
        await conn.close()


async def write_hubspot(entity_id: str, source_system: str) -> dict:
    if not ENRICHMENT_DB_URL:
        return {"status": "skipped", "reason": "DATABASE_URL not configured"}
    if not HUBSPOT_SYNC_URL:
        return {"status": "skipped", "reason": "HUBSPOT_SYNC_URL not configured"}

    conn = await asyncpg.connect(ENRICHMENT_DB_URL)
    try:
        entity = await conn.fetchrow(
            """
            SELECT id::text AS id, entity_type, label, source_system, external_ids,
                   enrichment_round, overall_confidence
            FROM entities
            WHERE id=$1::uuid
            """,
            entity_id,
        )
        if not entity:
            return {"status": "skipped", "reason": "entity not found"}

        attrs = await conn.fetch(
            """
            SELECT attr_key, attr_value, attr_value_json, confidence, source_collector
            FROM entity_attributes
            WHERE entity_id=$1::uuid AND is_trusted=true
            ORDER BY confidence DESC
            """,
            entity_id,
        )
    finally:
        await conn.close()

    trusted_attributes: dict[str, dict] = {}
    for row in attrs:
        value = (
            row["attr_value_json"]
            if row["attr_value_json"] is not None
            else row["attr_value"]
        )
        trusted_attributes[row["attr_key"]] = {
            "value": value,
            "confidence": float(row["confidence"] or 0.0),
            "source_collector": row["source_collector"],
        }

    payload = {
        "entity": {
            "id": entity["id"],
            "entity_type": entity["entity_type"],
            "label": entity["label"],
            "source_system": source_system or entity["source_system"],
            "external_ids": entity["external_ids"] or {},
            "enrichment_round": int(entity["enrichment_round"] or 0),
            "overall_confidence": float(entity["overall_confidence"] or 0.0),
        },
        "trusted_attributes": trusted_attributes,
        "meta": {
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "pipeline": "intelligence-platform-v3",
        },
    }

    headers = {}
    if HUBSPOT_SYNC_TOKEN:
        headers["Authorization"] = f"Bearer {HUBSPOT_SYNC_TOKEN}"

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                HUBSPOT_SYNC_URL, json=payload, headers=headers
            )
            response.raise_for_status()
        return {
            "status": "synced",
            "trusted_field_count": len(trusted_attributes),
        }
    except Exception as exc:
        return {
            "status": "failed",
            "error": str(exc),
            "trusted_field_count": len(trusted_attributes),
        }

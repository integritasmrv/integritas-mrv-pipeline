
# === COLLECTORS ===

async def nominatim_collect(entity_id: str, entity_label: str, primary: dict, round_n: int) -> list[dict]:
    query_parts = [
        str(primary.get("legal_name") or primary.get("label") or entity_label or "").strip(),
        str(primary.get("city") or primary.get("hq_city") or "").strip(),
        str(primary.get("country") or primary.get("hq_country") or "").strip(),
    ]
    query = " ".join([p for p in query_parts if p]).strip()
    if not query:
        return []
    params = {
        "q": query, "format": "jsonv2", "addressdetails": 1, "limit": 1,
        "accept-language": "en",
    }
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                NOMINATIM_URL, params=params,
                headers={"User-Agent": "IntegritasMRV-IntelligenceBot/1.0"},
            )
            resp.raise_for_status()
            rows = resp.json()
    except Exception as exc:
        log.warning("Nominatim failed for %s: %s", entity_id, exc)
        return []
    if not rows:
        return []
    hit = rows[0] if isinstance(rows, list) else {}
    addr = hit.get("address") or {}
    structured = {
        "query": query,
        "nominatim_display_name": hit.get("display_name"),
        "lat": str(hit.get("lat") or "").strip(),
        "lng": str(hit.get("lon") or "").strip(),
        "address": {
            "road": addr.get("road") or "",
            "house_number": addr.get("house_number") or "",
            "city": addr.get("city") or addr.get("town") or addr.get("village") or addr.get("municipality") or "",
            "postcode": addr.get("postcode") or "",
            "country": addr.get("country") or "",
            "country_code": str(addr.get("country_code") or "").lower(),
        },
        "osm_type": hit.get("osm_type"),
        "osm_id": hit.get("osm_id"),
    }
    return [{
        "entity_id": entity_id,
        "collector_name": "nominatim_geocode",
        "content_type": "json",
        "raw_content": json.dumps(structured)[:20000],
        "source_url": NOMINATIM_URL,
        "source_weight": 0.95,
        "round_number": round_n,
    }]


def _to_float(v: object) -> float | None:
    try:
        return float(str(v).strip())
    except Exception:
        return None


async def _nominatim_fallback_geocode(primary: dict, entity_label: str) -> tuple[float | None, float | None]:
    query_parts = [
        str(primary.get("legal_name") or primary.get("label") or entity_label or "").strip(),
        str(primary.get("city") or primary.get("hq_city") or "").strip(),
        str(primary.get("country") or primary.get("hq_country") or "").strip(),
    ]
    query = " ".join([p for p in query_parts if p]).strip()
    if not query:
        return None, None
    params = {"q": query, "format": "jsonv2", "addressdetails": 1, "limit": 1, "accept-language": "en"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(NOMINATIM_URL, params=params,
                headers={"User-Agent": "IntegritasMRV-IntelligenceBot/1.0"})
            resp.raise_for_status()
            rows = resp.json()
        if not rows:
            return None, None
        hit = rows[0]
        return _to_float(hit.get("lat")), _to_float(hit.get("lon"))
    except Exception:
        return None, None


OVERPASS_QUERY_TEMPLATE = """[out:json][timeout:{timeout}];
(
  node["name"~"{name}",i](around:{radius},{lat},{lon});
  way["name"~"{name}",i](around:{radius},{lat},{lon});
  relation["name"~"{name}",i](around:{radius},{lat},{lon});
);
out body;
"""


async def overpass_collect(entity_id: str, entity_label: str, primary: dict, round_n: int) -> list[dict]:
    company_name = str(primary.get("legal_name") or primary.get("label") or entity_label or "").strip()
    if not company_name:
        return []
    lat = _to_float(primary.get("lat") or primary.get("hq_lat"))
    lng = _to_float(primary.get("lng") or primary.get("hq_lng") or primary.get("lon"))
    if lat is None or lng is None:
        lat, lng = await _nominatim_fallback_geocode(primary, entity_label)
    if lat is None or lng is None:
        return []
    overpass_query = OVERPASS_QUERY_TEMPLATE.format(
        timeout=25, name=company_name.replace('"', "").replace("\n", " "),
        radius=100, lat=lat, lon=lng,
    )
    try:
        async with httpx.AsyncClient(timeout=35) as client:
            resp = await client.post(OVERPASS_URL, data={"data": overpass_query},
                headers={"User-Agent": "IntegritasMRV-IntelligenceBot/1.0"})
            resp.raise_for_status()
            payload = resp.json()
    except Exception as exc:
        log.warning("Overpass failed for %s: %s", entity_id, exc)
        return []
    elements = payload.get("elements") or []
    websites: list[str] = []
    results: list[dict] = []
    for el in elements:
        tags = el.get("tags") or {}
        phone = tags.get("phone") or tags.get("contact:phone") or tags.get("telephone")
        website = tags.get("website") or tags.get("contact:website") or tags.get("url")
        if not phone and not website:
            continue
        item = {
            "osm_id": el.get("id"), "osm_type": el.get("type"),
            "name": tags.get("name"), "phone": phone, "website": website,
            "addr_street": tags.get("addr:street"),
            "addr_housenumber": tags.get("addr:housenumber"),
            "addr_city": tags.get("addr:city"),
            "addr_postcode": tags.get("addr:postcode"),
            "addr_country": tags.get("addr:country"),
        }
        if website:
            websites.append(str(website))
        results.append(item)
    if not results:
        return []
    structured = {"query_name": company_name, "lat": lat, "lng": lng, "results": results}
    return [{
        "entity_id": entity_id,
        "collector_name": "overpass_lookup",
        "content_type": "json",
        "raw_content": json.dumps(structured)[:20000],
        "source_url": OVERPASS_URL,
        "source_weight": 0.75,
        "round_number": round_n,
    }]


async def scrapiq_collect(entity_id: str, entity_label: str, business_key: str, round_n: int) -> list[dict]:
    if not SCRAPIQ_DISPATCH_URL:
        return []
    scrapiq_name = BUSINESS_TO_SCRAPIQ.get(business_key, "integritas")
    if not scrapiq_name:
        return []
    payload = {"entity_id": entity_id, "entity_label": entity_label,
                "scrapiq_name": scrapiq_name, "round": round_n}
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(SCRAPIQ_DISPATCH_URL, json=payload,
                headers={"Content-Type": "application/json"})
            if resp.status_code not in (200, 201, 202):
                log.warning("Scrapiq dispatch failed for %s: %s", entity_id, resp.status_code)
                return []
    except Exception as exc:
        log.warning("Scrapiq dispatch exception for %s: %s", entity_id, exc)
        return []
    conn = await get_enrichment_conn()
    try:
        rows = await conn.fetch(
            """SELECT id, collector_name, source_url, content_type, raw_content
               FROM raw_evidence WHERE entity_id=$1 AND collector_name LIKE 'scrapiq_%%'
               ORDER BY scraped_at DESC LIMIT 20""", entity_id)
        return [{"entity_id": entity_id,
                 "collector_name": r["collector_name"],
                 "content_type": r["content_type"],
                 "raw_content": r["raw_content"][:20000],
                 "source_url": r["source_url"] or "",
                 "source_weight": 0.7,
                 "round_number": round_n} for r in rows]
    finally:
        await conn.close()


async def social_analyzer_collect(entity_id: str, entity_label: str, round_n: int) -> list[dict]:
    if not SOCIAL_ANALYZER_URL:
        return []
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(f"{SOCIAL_ANALYZER_URL}/analyze",
                params={"name": entity_label, "entity_id": entity_id},
                headers={"User-Agent": "IntegritasMRV-IntelligenceBot/1.0"})
            if resp.status_code != 200:
                return []
            data = resp.json()
    except Exception:
        return []
    profiles = data.get("profiles") or []
    if not profiles:
        return []
    return [{
        "entity_id": entity_id,
        "collector_name": "social_analyzer_api",
        "content_type": "json",
        "raw_content": json.dumps({"profiles": profiles})[:20000],
        "source_url": SOCIAL_ANALYZER_URL,
        "source_weight": 0.7,
        "round_number": round_n,
    }]


async def run_collectors(entity_id: str, entity_label: str, primary: dict,
                         business_key: str, round_n: int) -> list[dict]:
    all_evidence = []
    tasks = [
        nominatim_collect(entity_id, entity_label, primary, round_n),
        overpass_collect(entity_id, entity_label, primary, round_n),
        scrapiq_collect(entity_id, entity_label, business_key, round_n),
        social_analyzer_collect(entity_id, entity_label, round_n),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, list):
            all_evidence.extend(result)
        elif isinstance(result, Exception):
            log.warning("Collector exception: %s", result)
    return all_evidence


# === STORE EVIDENCE ===
async def store_evidence(conn: asyncpg.Connection, evidence_list: list[dict]) -> int:
    inserted = 0
    for ev in evidence_list:
        try:
            result = await conn.execute(
                """INSERT INTO raw_evidence
                   (entity_id, collector_name, source_url, content_type, raw_content,
                    source_weight, round_number)
                   VALUES ($1::uuid, $2, $3, $4, $5, $6, $7)""",
                ev["entity_id"], ev["collector_name"], ev.get("source_url", ""),
                ev["content_type"], ev["raw_content"],
                ev.get("source_weight", 0.5), ev.get("round_number", 1),
            )
            if str(result).endswith("1"):
                inserted += 1
        except Exception as exc:
            log.warning("store_evidence failed: %s", exc)
    return inserted

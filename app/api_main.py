import json
import os
import time

import asyncpg
from fastapi import FastAPI, Header, HTTPException, Query
from temporalio.client import Client

from app.workflows.crawl_workflow import CrawlWorkflow
from app.workflows.enrichment_workflow import EnrichmentWorkflow
from app.workflows.spiderfoot_workflow import SpiderFootWorkflow

TEMPORAL_ADDRESS = os.environ.get(
    "TEMPORAL_HOST",
    os.environ.get("TEMPORAL_ADDRESS", "temporal-oo48k0844ok8k4cwc8o8wgk8:7233"),
)
TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")
SCRAPIQ_DB_URL = os.environ.get(
    "SCRAPIQ_DATABASE_URL", os.environ.get("SCRAPIQ_DB_DSN", "")
)
ENRICHMENT_DB_URL = os.environ.get(
    "DATABASE_URL", os.environ.get("ENRICHMENT_DB_DSN", "")
)
SCRAPIQ_DISPATCH_TOKEN = os.environ.get("SCRAPIQ_DISPATCH_TOKEN", "")
DEFAULT_BUSINESS_KEY = os.environ.get("BUSINESS_KEY", "integritasmrv")
VALID_BUSINESS_KEYS = {"integritasmrv", "poweriq", "airbnb", "private", "ev-batteries"}

app = FastAPI(title="Intelligence Platform API", version="0.2.0")


def _normalize_request_business(payload: dict, source_system: str) -> str:
    direct = str(payload.get("request_business") or "").strip().lower()
    if direct:
        if direct in {"integritas", "integritasmrv", "forestfuture"}:
            return "integritas"
        if direct in {"pwriq", "pwr-iq", "wakongo"}:
            return "pwriq"
        if direct in {"airbnb", "belinus", "airrbnb", "airbnb"}:
            return "airbnb"
        if direct == "admin":
            return "admin"
        if direct == "sort":
            return "sort"
        return "integritas"
    ss = (source_system or "").lower()
    if "admin" in ss:
        return "admin"
    if "sort" in ss:
        return "sort"
    if "pwr" in ss or "wakongo" in ss:
        return "pwriq"
    if "air" in ss or "belinus" in ss:
        return "airbnb"
    return "integritas"


async def _enforce_business_access(
    conn: asyncpg.Connection, user_id: int | None, request_business: str
) -> None:
    if not user_id:
        return
    has_rows = await conn.fetchval(
        "SELECT EXISTS(SELECT 1 FROM nb_scrapiq_user_business_access WHERE user_id=$1)",
        user_id,
    )
    if not has_rows:
        return
    allowed = await conn.fetchval(
        """
        SELECT EXISTS(
          SELECT 1 FROM nb_scrapiq_user_business_access
          WHERE user_id=$1 AND business_code=$2
        )
        """,
        user_id,
        request_business,
    )
    if not allowed:
        raise HTTPException(
            status_code=403,
            detail=f"User {user_id} is not allowed for business '{request_business}'",
        )


async def _allowed_businesses(
    conn: asyncpg.Connection, user_id: int | None
) -> list[str] | None:
    if not user_id:
        return None
    rows = await conn.fetch(
        "SELECT business_code FROM nb_scrapiq_user_business_access WHERE user_id=$1",
        user_id,
    )
    if not rows:
        return None
    return [str(r["business_code"]) for r in rows]


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.post("/api/enrichment/trigger/{entity_id}")
async def trigger_enrichment(
    entity_id: str, source_system: str, reason: str = "manual", business_key: str = ""
) -> dict:
    bk = business_key or DEFAULT_BUSINESS_KEY
    if bk not in VALID_BUSINESS_KEYS:
        raise HTTPException(status_code=400, detail=f"Invalid business_key: {bk}")

    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    handle = await client.start_workflow(
        EnrichmentWorkflow.run,
        args=[entity_id, source_system, 1, bk],
        id=f"enrich-{entity_id}-{reason}-{int(time.time())}",
        task_queue=f"enrichment-{bk}",
    )
    return {
        "status": "triggered",
        "workflow_id": handle.id,
        "business_key": bk,
    }


@app.post("/api/enrichment/trigger")
async def trigger_enrichment_payload(payload: dict) -> dict:
    if not ENRICHMENT_DB_URL:
        return {"status": "error", "message": "DATABASE_URL not configured"}

    business_key = str(payload.get("business_key") or DEFAULT_BUSINESS_KEY).strip()
    if business_key not in VALID_BUSINESS_KEYS:
        return {"status": "error", "message": f"Invalid business_key: {business_key}"}

    entity_type = str(payload.get("entity_type") or "company").strip() or "company"
    label = str(payload.get("label") or "Untitled Entity").strip() or "Untitled Entity"
    source_system = str(payload.get("source_system") or "crm-forestfuture").strip()
    attributes = payload.get("attributes") or {}
    external_ids = payload.get("external_ids") or {}

    conn = await asyncpg.connect(ENRICHMENT_DB_URL)
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO entities (entity_type, label, enrichment_status, source_system, external_ids, business_key)
            VALUES ($1, $2, 'in_progress', $3, $4::jsonb, $5)
            RETURNING id::text AS id
            """,
            entity_type,
            label,
            source_system,
            json.dumps(external_ids),
            business_key,
        )
        entity_id = str(row["id"])

        if isinstance(attributes, dict):
            blocked = {"opening_hours", "business_hours", "hours_of_operation"}
            for key, value in attributes.items():
                attr_key = str(key or "").strip()
                if not attr_key or attr_key in blocked:
                    continue
                attr_value = None if value is None else str(value)
                await conn.execute(
                    """
                    INSERT INTO entity_attributes
                      (entity_id, attr_key, attr_value, confidence, round_number, extraction_method, source_collector, is_trusted, business_key)
                    VALUES ($1::uuid, $2, $3, 0.9, 1, 'api_seed', 'api_trigger', true, $4)
                    ON CONFLICT DO NOTHING
                    """,
                    entity_id,
                    attr_key,
                    attr_value,
                    business_key,
                )
    finally:
        await conn.close()

    temporal = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    handle = await temporal.start_workflow(
        EnrichmentWorkflow.run,
        args=[entity_id, source_system, 1, business_key],
        id=f"enrich-{entity_id}-api-{int(time.time())}",
        task_queue=f"enrichment-{business_key}",
    )

    return {
        "status": "triggered",
        "entity_id": entity_id,
        "workflow_id": handle.id,
        "business_key": business_key,
    }


@app.get("/api/enrichment/status/{entity_id}")
async def get_enrichment_status(entity_id: str) -> dict:
    if not ENRICHMENT_DB_URL:
        return {"status": "error", "message": "DATABASE_URL not configured"}
    conn = await asyncpg.connect(ENRICHMENT_DB_URL)
    try:
        row = await conn.fetchrow(
            "SELECT id::text, entity_type, label, enrichment_status, business_key, source_system, created_at, updated_at FROM entities WHERE id::text = $1",
            entity_id,
        )
        if not row:
            return {"status": "not_found", "entity_id": entity_id}
        return dict(row)
    finally:
        await conn.close()


@app.post("/api/scrapiq/dispatch")
@app.post("/api/scrapiq-dispatch")
async def dispatch_scrapiq(
    payload: dict, authorization: str | None = Header(default=None)
) -> dict:
    if SCRAPIQ_DISPATCH_TOKEN:
        expected = f"Bearer {SCRAPIQ_DISPATCH_TOKEN}"
        if authorization != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")

    if not SCRAPIQ_DB_URL:
        return {"status": "error", "message": "SCRAPIQ_DATABASE_URL not configured"}

    request_type = str(payload.get("request_type") or "web_page_discovery")
    prompt = str(payload.get("prompt") or "").strip() or "Research request"
    starting_url = str(payload.get("starting_url") or "").strip() or None
    search_scope = str(payload.get("search_scope") or "full_web")
    source_system = str(payload.get("source_system") or "crm-forestfuture")
    request_business = _normalize_request_business(payload, source_system)
    initiated_by_user_id = payload.get("initiated_by_user_id")
    user_id = int(initiated_by_user_id) if initiated_by_user_id is not None else None
    enrichment_entity_id = payload.get("enrichment_entity_id")
    enrichment_round = int(payload.get("enrichment_round") or 1)
    context_entity_type = str(payload.get("context_entity_type") or "") or None
    context_entity_name = str(payload.get("context_entity_name") or "") or None
    context_entity_data = payload.get("context_entity_data")
    context_entity_data_json = (
        json.dumps(context_entity_data) if context_entity_data is not None else None
    )
    initiated_by_service = str(
        payload.get("initiated_by_service") or "enrichment-pipeline"
    )
    max_results = int(payload.get("max_results") or 20)
    min_relevance = float(payload.get("min_relevance_score") or 0.45)
    summarize = bool(payload.get("summarize_results", True))
    llm_alias = str(payload.get("llm_alias") or "summarizer")

    conn = await asyncpg.connect(SCRAPIQ_DB_URL)
    try:
        await _enforce_business_access(conn, user_id, request_business)
        row = await conn.fetchrow(
            """
            INSERT INTO nb_scrapiq_requests
              (request_type, initiator_type, initiated_by_service, prompt,
               starting_url, search_scope, status, max_results, min_relevance_score,
               summarize_results, llm_alias_used, source_system, request_business, initiated_by_user_id, enrichment_entity_id,
               enrichment_round, context_entity_type, context_entity_name, context_entity_data)
            VALUES
              ($1, 'external_service', $2, $3,
               $4, $5, 'scheduled', $6, $7,
               $8, $9, $10, $11, $12, $13::uuid,
               $14, $15, $16, $17::jsonb)
            RETURNING id, request_no
            """,
            request_type,
            initiated_by_service,
            prompt,
            starting_url,
            search_scope,
            max_results,
            min_relevance,
            summarize,
            llm_alias,
            source_system,
            request_business,
            user_id,
            enrichment_entity_id,
            enrichment_round,
            context_entity_type,
            context_entity_name,
            context_entity_data_json,
        )
    finally:
        await conn.close()

    request_id = int(row["id"])
    request_no = str(row["request_no"])

    temporal = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    if request_type == "spiderfoot_scan":
        target = starting_url or prompt
        handle = await temporal.start_workflow(
            SpiderFootWorkflow.run,
            args=[request_id, target],
            id=f"spiderfoot-{request_id}-{int(time.time())}",
            task_queue=f"spiderfoot-{request_business}",
        )
    else:
        handle = await temporal.start_workflow(
            CrawlWorkflow.run,
            args=[request_id],
            id=f"crawl-{request_id}-{int(time.time())}",
            task_queue=f"crawl-{request_business}",
        )

    return {
        "status": "accepted",
        "request_id": request_id,
        "request_no": request_no,
        "workflow_id": handle.id,
    }


@app.get("/api/scrapiq/businesses")
async def list_scrapiq_businesses() -> dict:
    if not SCRAPIQ_DB_URL:
        return {"status": "error", "message": "SCRAPIQ_DATABASE_URL not configured"}
    conn = await asyncpg.connect(SCRAPIQ_DB_URL)
    try:
        rows = await conn.fetch(
            """
            SELECT code, name, is_active
            FROM nb_scrapiq_businesses
            WHERE is_active = true
            ORDER BY code
            """
        )
        return {"status": "ok", "items": [dict(r) for r in rows]}
    finally:
        await conn.close()


@app.get("/api/scrapiq/requests")
async def list_scrapiq_requests(
    user_id: int | None = Query(default=None),
    request_business: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict:
    if not SCRAPIQ_DB_URL:
        return {"status": "error", "message": "SCRAPIQ_DATABASE_URL not configured"}

    conn = await asyncpg.connect(SCRAPIQ_DB_URL)
    try:
        allowed = await _allowed_businesses(conn, user_id)
        where = ["1=1"]
        params: list[object] = []

        if request_business:
            params.append(request_business)
            where.append(f"request_business = ${len(params)}")
        if status:
            params.append(status)
            where.append(f"status = ${len(params)}")

        if allowed is not None:
            params.append(allowed)
            where.append(f"request_business = ANY(${len(params)}::text[])")

        params.append(limit)
        sql = (
            "SELECT id, request_no, request_business, request_type, status, source_system, "
            "initiated_at, started_at, finished_at, results_found, results_processed, results_failed "
            "FROM nb_scrapiq_requests "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY id DESC LIMIT $" + str(len(params))
        )
        rows = await conn.fetch(sql, *params)
        return {
            "status": "ok",
            "count": len(rows),
            "items": [dict(r) for r in rows],
            "restricted": allowed is not None,
        }
    finally:
        await conn.close()

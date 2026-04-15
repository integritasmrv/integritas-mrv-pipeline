from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import Optional
import asyncio
import asyncpg
from temporalio.client import Client


app = FastAPI(title="IntegritasMRV Temporal Bridge")

TEMPORAL_ADDR = "10.0.4.16:7233"
TASK_QUEUE = "integritasmrv-ingest"


class HubspotWebhookPayload(BaseModel):
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


class EnrichIQWriteback(BaseModel):
    entity: dict
    trusted_attributes: dict
    meta: dict


@app.post("/webhook/hubspot")
async def webhook_hubspot(request: Request):
    try:
        body = await request.json()
        print(f"[WEBHOOK] raw body: {str(body)[:500]}", flush=True)

        items = []
        if isinstance(body, list):
            items = body
        elif isinstance(body, dict):
            objs = body.get("objects")
            if objs and isinstance(objs, list):
                items = objs
            elif "objectId" in body or "properties" in body:
                items = [body]
            elif "data" in body:
                items = [{"properties": body["data"].get("properties", body["data"])}]
            else:
                items = [body]

        conn = await asyncpg.connect(
            host="10.0.13.2", port=5432, database="integritasmrv_crm",
            user="integritasmrv_crm_user", password="Int3gr1t@smrv_S3cure_P@ssw0rd_2026"
        )

        client = await Client.connect(TEMPORAL_ADDR)
        results = []

        for item in items:
            props = item.get("properties", {})
            object_type = item.get("objectType", item.get("object_type", "contact")).lower()
            object_id = item.get("objectId", item.get("hs_object_id", props.get("hs_object_id", "")))
            owner_id = props.get("hubspot_owner_id", "")

            print(f"[WEBHOOK] type={object_type} id={object_id} owner={owner_id}", flush=True)

            route_row = await conn.fetchrow(
                "SELECT target_crm FROM crm_sync_routing WHERE hubspot_owner_id = $1 AND active = true LIMIT 1",
                str(owner_id)
            )

            if route_row:
                target_crm = route_row["target_crm"].lower()
            else:
                target_crm = "integritasmrv"

            mapping_name = "hubspot_to_crm"
            business_key = str(object_id) if object_id else None

            wf_id = await client.start_workflow(
                "IngestWorkflow",
                {
                    "source": "hubspot",
                    "mapping_name": mapping_name,
                    "target_crm": target_crm,
                    "business_key": business_key,
                    "data": {"properties": props, "objectType": object_type, "objectId": object_id},
                },
                id=f"ingest-hubspot-{business_key}",
                task_queue=TASK_QUEUE,
            )

            results.append({"objectId": object_id, "status": "accepted", "target_crm": target_crm})

        await conn.close()
        return {"status": "ok", "processed": len(results), "results": results}
    except Exception as e:
        print(f"[WEBHOOK] Error: {e}", flush=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/writeback")
async def writeback(payload: WritebackPayload, request: Request):
    try:
        client = await Client.connect(TEMPORAL_ADDR)
        wf_id = await client.start_workflow(
            "WritebackWorkflow",
            {
                "entity_id": payload.entity_id,
                "target_crm": payload.target_crm,
                "source_system": payload.source_system,
                "entity_type": payload.entity_type,
                "enriched_data": payload.enriched_data,
                "external_ids": payload.external_ids,
            },
            id=f"writeback-{payload.entity_id}",
            task_queue=TASK_QUEUE,
        )
        return {"status": "accepted", "workflow_id": str(wf_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/enrichiq/writeback")
async def enrichiq_writeback(payload: EnrichIQWriteback, request: Request):
    try:
        meta = payload.meta or {}
        entity_id = meta.get("entity_id")
        target_crm = meta.get("target_crm", "integritasmrv")
        source_system = meta.get("source_system", "hubspot")
        entity_type = meta.get("entity_type", "contact")
        external_ids = meta.get("external_ids", {})

        if not entity_id:
            raise HTTPException(status_code=400, detail="entity_id required in meta")

        enriched_data = {
            "entity_attributes": payload.entity or {},
            "enrichment_score": payload.trusted_attributes.get("enrichment_score"),
            "last_enriched_at": payload.trusted_attributes.get("last_enriched_at"),
        }

        client = await Client.connect(TEMPORAL_ADDR)
        wf_id = await client.start_workflow(
            "WritebackWorkflow",
            {
                "entity_id": entity_id,
                "target_crm": target_crm,
                "source_system": source_system,
                "entity_type": entity_type,
                "enriched_data": enriched_data,
                "external_ids": external_ids,
            },
            id=f"writeback-enrichiq-{entity_id}",
            task_queue=TASK_QUEUE,
        )
        return {"status": "accepted", "workflow_id": str(wf_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    return {"status": "ok", "temporal": TEMPORAL_ADDR, "namespace": "Integritasmrv"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=15579)

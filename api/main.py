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
async def webhook_hubspot(payload: HubspotWebhookPayload, request: Request):
    try:
        owner_id = payload.data.get("properties", {}).get("hubspot_owner_id", "")
        print(f"[WEBHOOK] owner_id={owner_id}, object_id={payload.data.get('properties', {}).get('hs_object_id', '')}")

        conn = await asyncpg.connect(
            host="10.0.13.2", port=5432, database="integritasmrv_crm",
            user="integritasmrv_crm_user", password="Int3gr1t@smrv_S3cure_P@ssw0rd_2026"
        )
        route_row = await conn.fetchrow(
            "SELECT target_crm FROM crm_sync_routing WHERE hubspot_owner_id = $1 AND active = true LIMIT 1",
            str(owner_id)
        )
        await conn.close()

        if route_row:
            target_crm = route_row["target_crm"].lower()
        else:
            target_crm = payload.data.get("routing_rules", {}).get("target_crm", "integritasmrv")

        business_key = payload.data.get("properties", {}).get("hs_object_id")

        client = await Client.connect(TEMPORAL_ADDR)
        wf_id = await client.start_workflow(
            "IngestWorkflow",
            {
                "source": payload.source,
                "mapping_name": "hubspot_to_crm",
                "target_crm": target_crm,
                "business_key": str(business_key) if business_key else None,
                "data": payload.data,
            },
            id=f"ingest-hubspot-{business_key}",
            task_queue=TASK_QUEUE,
        )

        return {"status": "accepted", "workflow_id": str(wf_id), "target_crm": target_crm, "owner_id": owner_id}
    except Exception as e:
        print(f"[WEBHOOK] Error: {e}")
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

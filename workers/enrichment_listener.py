import asyncio
import asyncpg
import sys
import json
from datetime import datetime
from temporalio.client import Client


CRM_DBS = [
    {
        "name": "integritasmrv",
        "host": "10.0.13.2",
        "port": 5432,
        "db": "integritasmrv_crm",
        "user": "integritasmrv_crm_user",
        "password": "Int3gr1t@smrv_S3cure_P@ssw0rd_2026",
    },
    {
        "name": "poweriq",
        "host": "10.0.14.2",
        "port": 5432,
        "db": "poweriq_crm",
        "user": "poweriq_crm_user",
        "password": "P0w3r1Q_CRM_S3cur3_P@ss_2026",
    },
]

NOTIFICATION_QUEUE = "enrichment_queue"
TASK_QUEUE = "integritasmrv-ingest"
TEMPORAL_ADDR = "10.0.4.16:7233"

temporal_client = None


async def get_temporal_client():
    global temporal_client
    if temporal_client is None:
        temporal_client = await Client.connect(TEMPORAL_ADDR)
    return temporal_client


async def handle_notification(crm_name: str, payload_str: str):
    try:
        parts = payload_str.split("|")
        if len(parts) < 2:
            print(f"[{crm_name}] Invalid payload: {payload_str}", flush=True)
            return

        entity_id = int(parts[0])
        hubspot_id = parts[1] if len(parts) > 1 else None
        timestamp = parts[2] if len(parts) > 2 else datetime.utcnow().isoformat()

        client = await get_temporal_client()
        wf_id = f"enrich-{crm_name}-{entity_id}-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}"
        await client.start_workflow(
            "EnrichmentWorkflow",
            {
                "entity_id": entity_id,
                "crm_name": crm_name,
                "hubspot_id": hubspot_id,
                "timestamp": timestamp,
            },
            id=wf_id,
            task_queue=TASK_QUEUE,
        )
        print(f"[{crm_name}] Triggered EnrichmentWorkflow for entity {entity_id}", flush=True)
    except Exception as e:
        print(f"[{crm_name}] Error: {e}", flush=True)


async def listen_crm(db: dict):
    crm_name = db["name"]
    conn = await asyncpg.connect(
        host=db["host"],
        port=db["port"],
        database=db["db"],
        user=db["user"],
        password=db["password"],
    )

    loop = asyncio.get_event_loop()

    def notification_handler(connection, channel, payload, worker_id):
        loop.create_task(handle_notification(crm_name, payload))

    await conn.add_listener(NOTIFICATION_QUEUE, notification_handler)
    print(f"[{crm_name}] Listening on {NOTIFICATION_QUEUE}...", flush=True)

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await conn.close()


async def main():
    print("Starting enrichment listener...", flush=True)
    await asyncio.gather(*[listen_crm(db) for db in CRM_DBS])


if __name__ == "__main__":
    asyncio.run(main())

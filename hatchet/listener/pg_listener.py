import asyncio
import json
import logging
import os
import signal
import sys

import asyncpg
from hatchet_sdk import Hatchet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [pg-listener] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

CRM_DSN = os.environ.get(
    "CRM_DSN",
    "postgresql://integritasmrv_crm_user:oYxxPKRfAHAD263VSDcKmljKY0vInx2QTl6PooKoqmmiDops@crm-integritasmrv-db:5432/integritasmrv_crm",
)
HATCHET_TOKEN = os.environ.get("HATCHET_CLIENT_TOKEN", "")
HATCHET_HOST = os.environ.get("HATCHET_CLIENT_HOST_PORT", "hatchet-engine:7070")
CHANNEL = "enrichment_queue"

os.environ.setdefault("HATCHET_CLIENT_TOKEN", HATCHET_TOKEN)
os.environ.setdefault("HATCHET_CLIENT_HOST_PORT", HATCHET_HOST)
os.environ.setdefault("HATCHET_CLIENT_TLS_STRATEGY", "none")


_hatchet = None


async def main():
    global _hatchet
    _hatchet = Hatchet()
    log.info("Connecting to CRM database...")
    conn = await asyncpg.connect(CRM_DSN)
    log.info("Connected. Listening on channel: %s", CHANNEL)

    await conn.add_listener(CHANNEL, _on_notification)

    stop_event = asyncio.Event()

    def _signal_handler(*_):
        stop_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    log.info("pg_listener ready — waiting for notifications")
    await stop_event.wait()

    await conn.remove_listener(CHANNEL, _on_notification)
    await conn.close()
    log.info("Shut down cleanly")


def _on_notification(conn, pid, channel, payload):
    try:
        data = json.loads(payload)
    except (json.JSONDecodeError, TypeError):
        log.warning("Invalid payload on %s: %s", channel, payload)
        return

    company_id = data.get("id")
    name = data.get("name", "unknown")
    log.info(
        "Received notification: id=%s name=%s action=%s",
        company_id,
        name,
        data.get("action"),
    )

    try:
        workflow_input = {
            "company_id": str(company_id),
            "name": data.get("name") or "",
            "website": data.get("website"),
            "country": data.get("country"),
            "industry": data.get("industry"),
            "hubspot_id": data.get("hubspot_id"),
            "company_email": data.get("company_email"),
        }
        result = _hatchet.event.push("enrichment-request", workflow_input)
        log.info("Pushed to Hatchet: id=%s event_id=%s", company_id, getattr(result, "eventId", result))
    except Exception as exc:
        log.error("Failed to push to Hatchet: id=%s error=%s", company_id, exc)


if __name__ == "__main__":
    asyncio.run(main())

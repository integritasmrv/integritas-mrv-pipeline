import asyncio
import json
import logging
import os
import signal

import asyncpg
from hatchet_sdk import Hatchet

logging.basicConfig(level=logging.INFO, format="%(asctime)s [pg-listener] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

HATCHET_TOKEN = os.environ.get("HATCHET_CLIENT_TOKEN", "")
HATCHET_HOST = os.environ.get("HATCHET_CLIENT_HOST_PORT", "hatchet-engine:7070")
os.environ.setdefault("HATCHET_CLIENT_TOKEN", HATCHET_TOKEN)
os.environ.setdefault("HATCHET_CLIENT_HOST_PORT", HATCHET_HOST)
os.environ.setdefault("HATCHET_CLIENT_TLS_STRATEGY", "none")

CHANNELS = ["enrichment_queue", "contact_enrichment_queue"]

CRM_CONFIGS = {
    "integritasmrv": {
        "host": os.environ.get("CRM_INTEGRITASMRV_HOST", "127.0.0.1"),
        "port": int(os.environ.get("CRM_INTEGRITASMRV_PORT", "15432")),
        "user": os.environ.get("CRM_INTEGRITASMRV_USER", "integritasmrv_crm_user"),
        "password": os.environ.get("CRM_INTEGRITASMRV_PASS", "oYxxPKRfAHAD263VSDcKmljKY0vInx2QTl6PooKoqmmiDops"),
        "database": os.environ.get("CRM_INTEGRITASMRV_DB", "integritasmrv_crm"),
    },
    "poweriq": {
        "host": os.environ.get("CRM_POWERIQ_HOST", "127.0.0.1"),
        "port": int(os.environ.get("CRM_POWERIQ_PORT", "15433")),
        "user": os.environ.get("CRM_POWERIQ_USER", "poweriq_crm_user"),
        "password": os.environ.get("CRM_POWERIQ_PASS", "P0w3r1Q_CRM_S3cur3_P@ss_2026"),
        "database": os.environ.get("CRM_POWERIQ_DB", "poweriq_crm"),
    },
}

_hatchet = None


def make_handler(crm_name):
    def _on_notification(conn, pid, channel, payload):
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            log.warning("Invalid payload on %s/%s: %s", crm_name, channel, payload)
            return
        data["crm_name"] = crm_name
        if channel == "enrichment_queue":
            company_id = data.get("id")
            name = data.get("name", "unknown")
            log.info("[%s] Company: id=%s name=%s action=%s", crm_name, company_id, name, data.get("action"))
            try:
                wi = {
                    "company_id": str(company_id),
                    "name": data.get("name") or "",
                    "website": data.get("website"),
                    "country": data.get("country"),
                    "industry": data.get("industry"),
                    "hubspot_id": data.get("hubspot_id"),
                    "company_email": data.get("company_email"),
                    "crm_name": crm_name,
                }
                result = _hatchet.event.push("enrichment-request", wi)
                log.info("[%s] Pushed company: id=%s event_id=%s", crm_name, company_id, getattr(result, "event_id", str(result)))
            except Exception as exc:
                log.error("[%s] Failed to push company: id=%s error=%s", crm_name, company_id, exc)
        elif channel == "contact_enrichment_queue":
            contact_id = data.get("id")
            name = data.get("name", "unknown")
            log.info("[%s] Contact: id=%s name=%s action=%s", crm_name, contact_id, name, data.get("action"))
            try:
                wi = {
                    "contact_id": str(contact_id),
                    "name": data.get("name") or "",
                    "email": data.get("email"),
                    "phone": data.get("phone"),
                    "title": data.get("title"),
                    "linkedin": data.get("linkedin"),
                    "hubspot_id": data.get("hubspot_id"),
                    "crm_name": crm_name,
                }
                result = _hatchet.event.push("contact-enrichment-request", wi)
                log.info("[%s] Pushed contact: id=%s event_id=%s", crm_name, contact_id, getattr(result, "event_id", str(result)))
            except Exception as exc:
                log.error("[%s] Failed to push contact: id=%s error=%s", crm_name, contact_id, exc)

    return _on_notification


async def main():
    global _hatchet
    _hatchet = Hatchet()
    connections = []
    for crm_name, cfg in CRM_CONFIGS.items():
        log.info("Connecting to %s CRM at %s:%s/%s ...", crm_name, cfg["host"], cfg["port"], cfg["database"])
        try:
            conn = await asyncpg.connect(
                host=cfg["host"], port=cfg["port"],
                user=cfg["user"], password=cfg["password"],
                database=cfg["database"], ssl=None,
            )
            handler = make_handler(crm_name)
            for ch in CHANNELS:
                await conn.add_listener(ch, handler)
            connections.append((crm_name, conn, handler))
            log.info("[%s] Connected, listening on %s", crm_name, CHANNELS)
        except Exception as exc:
            log.error("[%s] Connection failed: %s", crm_name, exc)

    stop_event = asyncio.Event()

    def _signal_handler(*_):
        stop_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    log.info("pg_listener ready — %d CRM(s) connected", len(connections))
    await stop_event.wait()
    for crm_name, conn, handler in connections:
        for ch in CHANNELS:
            await conn.remove_listener(ch, handler)
        await conn.close()
    log.info("Shut down cleanly")


if __name__ == "__main__":
    asyncio.run(main())

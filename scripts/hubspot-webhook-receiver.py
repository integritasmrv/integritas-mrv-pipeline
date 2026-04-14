"""
hubspot-webhook-receiver.py
Lightweight Flask server that receives HubSpot webhooks and triggers Windmill flow.
Runs on server2:5000.
"""
import os, json, logging
from flask import Flask, request, abort
import httpx

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

WINDMILL_URL = os.environ.get("WINDMILL_URL", "http://144.91.126.111:8002")
WINDMILL_TOKEN = os.environ.get("WINDMILL_TOKEN", "")  # Set via env
FLOW_PATH = "f/enrichiq/pipeline"

# Map HubSpot object types to Windmill params
BUSINESS_KEY_MAP = {
    "CONTACT": "integritasmrv",
    "COMPANY": "integritasmrv",
}


@app.route("/webhook/hubspot", methods=["POST"])
def hubspot_webhook():
    try:
        payload = request.get_json(force=True)
    except Exception:
        abort(400, "Invalid JSON")

    log.info(f"HubSpot webhook received: {json.dumps(payload, indent=2)[:500]}")

    # Handle both single object and array formats
    objects = payload.get("objects", [payload]) if isinstance(payload, dict) else payload
    if not isinstance(objects, list):
        objects = [objects]

    results = []
    for obj in objects:
        object_type = obj.get("objectType", obj.get("object_type", "unknown")).upper()
        hs_id = obj.get("objectId", obj.get("id", ""))
        
        # Extract properties
        props = obj.get("properties", {})
        company = props.get("company", props.get("company_name", ""))
        firstname = props.get("firstname", "")
        lastname = props.get("lastname", "")
        email = props.get("email", "")
        label = f"{firstname} {lastname}".strip() or company or f"HubSpot {object_type}"
        
        business_key = BUSINESS_KEY_MAP.get(object_type, "integritasmrv")
        entity_type = "contact" if object_type == "CONTACT" else "company"

        # Call Windmill enrich-trigger script
        windmill_result = call_windmill_trigger(
            entity_id=hs_id or email,
            business_key=business_key,
            entity_type=entity_type,
            label=label,
            source_system="hubspot-webhook",
        )
        results.append(windmill_result)

    return {"status": "ok", "processed": len(results), "results": results}


def call_windmill_trigger(entity_id: str, business_key: str, entity_type: str, label: str, source_system: str) -> dict:
    """Trigger enrichment via Windmill script."""
    if not WINDMILL_TOKEN:
        log.warning("WINDMILL_TOKEN not set, skipping Windmill call")
        return {"entity_id": entity_id, "windmill": "skipped_no_token"}

    script_path = "u/admin/enrich-trigger"
    url = f"{WINDMILL_URL}/api/mcp/w/admins/scripts/{script_path}/run"

    payload = {
        "entity_id": entity_id,
        "business_key": business_key,
        "entity_type": entity_type,
        "label": label,
        "source_system": source_system,
    }

    try:
        resp = httpx.post(
            url,
            json=payload,
            headers={
                "Authorization": f"Bearer {WINDMILL_TOKEN}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=60,
        )
        resp.raise_for_status()
        result = resp.json()
        log.info(f"Windmill triggered: {result}")
        return {"entity_id": entity_id, "windmill": result}
    except httpx.HTTPStatusError as e:
        log.error(f"Windmill HTTP error: {e.response.status_code} - {e.response.text}")
        return {"entity_id": entity_id, "windmill": f"error: {e.response.status_code}"}
    except Exception as e:
        log.error(f"Windmill call failed: {e}")
        return {"entity_id": entity_id, "windmill": f"error: {e}"}


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

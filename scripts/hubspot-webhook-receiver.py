"""
hubspot-webhook-receiver.py
Receives HubSpot webhooks and triggers Windmill enrichment via MCP JSON-RPC.
Runs on server2:5000.
"""
import os
import json
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

WINDMILL_URL = os.environ.get("WINDMILL_URL", "http://127.0.0.1:8002")
WINDMILL_TOKEN = os.environ.get("WINDMILL_TOKEN", "")
PORT = int(os.environ.get("PORT", 5000))

BUSINESS_KEY_MAP = {
    "CONTACT": "integritasmrv",
    "COMPANY": "integritasmrv",
}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != '/webhook/hubspot':
            self.send_error(404, "Not Found")
            return

        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            payload = json.loads(body)
        except Exception as e:
            log.error("Failed to parse payload: %s", e)
            self.send_error(400, "Invalid JSON")
            return

        log.info("HubSpot webhook: %s", json.dumps(payload, indent=2)[:500])

        objects = payload.get('objects', [payload]) if isinstance(payload, dict) else payload
        if not isinstance(objects, list):
            objects = [objects]

        results = []
        for obj in objects:
            obj_type = obj.get('objectType', obj.get('object_type', 'unknown'))
            if obj_type:
                obj_type = obj_type.upper()
            else:
                obj_type = 'UNKNOWN'
            hs_id = obj.get('objectId', obj.get('id', ''))
            props = obj.get('properties', {})
            company = props.get('company', props.get('company_name', ''))
            firstname = props.get('firstname', '')
            lastname = props.get('lastname', '')
            email = props.get('email', '')
            label = f"{firstname} {lastname}".strip() or company or f"HubSpot {obj_type}"

            business_key = BUSINESS_KEY_MAP.get(obj_type, 'integritasmrv')
            entity_type = 'contact' if obj_type == 'CONTACT' else 'company'

            result = call_windmill_mcp(hs_id or email, business_key, entity_type, label)
            results.append(result)

        response = {'status': 'ok', 'processed': len(results), 'results': results}
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())

    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status": "ok"}')
        else:
            self.send_error(404)

    def log_message(self, fmt, *args):
        log.info(fmt % args)


def call_windmill_mcp(entity_id, business_key, entity_type, label):
    """Trigger enrichment via Windmill MCP JSON-RPC."""
    if not WINDMILL_TOKEN:
        log.warning("WINDMILL_TOKEN not set, skipping")
        return {'entity_id': entity_id, 'windmill': 'skipped'}

    payload = json.dumps({
        'jsonrpc': '2.0',
        'method': 'tools/call',
        'params': {
            'name': 'runScriptByPath',
            'arguments': {
                'path': 'u/admin/enrich-trigger',
                'args': {
                    'entity_id': str(entity_id),
                    'business_key': business_key,
                    'entity_type': entity_type,
                    'label': label,
                    'source_system': 'hubspot-webhook',
                }
            }
        },
        'id': 1
    }).encode()

    from urllib.request import Request, urlopen
    from urllib.error import HTTPError

    req = Request(
        WINDMILL_URL + "/api/mcp/w/admins/mcp",
        data=payload,
        headers={
            'Authorization': 'Bearer ' + WINDMILL_TOKEN,
            'Content-Type': 'application/json',
            'Accept': 'application/json, text/event-stream',
        },
        method='POST'
    )
    try:
        resp = urlopen(req, timeout=60)
        data = resp.read().decode()
        log.info("Windmill response: %s", data[:500])

        for line in data.split('\n'):
            if line.startswith('data: '):
                result = json.loads(line[6:])
                if 'error' in result:
                    return {'entity_id': entity_id, 'windmill': 'error', 'msg': result['error'].get('message', str(result['error']))}
                if 'result' in result:
                    content = result['result'].get('content', [])
                    if content and isinstance(content[0], dict):
                        return {'entity_id': entity_id, 'windmill': 'ok', 'job_id': content[0].get('text', '').strip('"')}
        return {'entity_id': entity_id, 'windmill': 'ok', 'raw': data[:200]}
    except HTTPError as e:
        body = e.read().decode() if e.fp else str(e)
        log.error("Windmill HTTP error %s: %s", e.code, body[:200])
        return {'entity_id': entity_id, 'windmill': 'error ' + str(e.code)}
    except Exception as e:
        log.error("Windmill call failed: %s", e)
        return {'entity_id': entity_id, 'windmill': 'error: ' + str(e)}


if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', PORT), Handler)
    log.info("HubSpot webhook receiver listening on port %s", PORT)
    server.serve_forever()

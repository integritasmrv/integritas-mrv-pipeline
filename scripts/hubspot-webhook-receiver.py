import os, json, logging
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import Request, urlopen
logging.basicConfig(level=logging.INFO)
log = logging.getLogger()
WURL = os.environ.get("WINDMILL_URL", "http://127.0.0.1:8002")
WTOKEN = os.environ.get("WINDMILL_TOKEN", "")
HTOKEN = os.environ.get("HUBSPOT_TOKEN", "")
PORT = int(os.environ.get("PORT", 5000))
OWNER_TO_BUSINESS_KEY = {
    "syncmrv@integritasmrv.com": "integritasmrv",
    "syncpower@integritasmrv.com": "poweriq",
}
def get_business_key(props):
    owner = props.get("hubspot_owner_id", "") or props.get("owner_email", "")
    return OWNER_TO_BUSINESS_KEY.get(owner.lower() if owner else "")
class H(BaseHTTPRequestHandler):
  def do_POST(self):
    if self.path != "/webhook/hubspot": self.send_error(404); return
    try:
      cl = int(self.headers.get("Content-Length", 0))
      p = json.loads(self.rfile.read(cl).decode())
    except: self.send_error(400); return
    log.info("Webhook: %s", str(p)[:200])
    objs = p.get("objects", [p]) if isinstance(p, dict) else p
    res = []
    for o in objs:
      ot = o.get("objectType", "unknown").upper()
      hid = o.get("objectId", "")
      pp = o.get("properties", {})
      co = pp.get("company", "")
      fn = pp.get("firstname", "")
      ln = pp.get("lastname", "")
      owner = pp.get("hubspot_owner_id", "") or pp.get("owner_email", "")
      lb = f"{fn} {ln}".strip() or co or f"HubSpot {ot}"
      bk = get_business_key(pp)
      if not bk:
        log.info("Skipping: owner %s not matched", owner)
        res.append({"entity_id": hid, "windmill": "skipped", "reason": "owner_not_matched"})
        continue
      et = "contact" if ot == "CONTACT" else "company"
      r = call_wm(hid, bk, et, lb)
      euuid = r.get("enrichiq_uuid")
      if euuid and hid: store_hs(ot, hid, euuid)
      res.append(r)
    self.send_response(200)
    self.send_header("Content-Type", "application/json")
    self.end_headers()
    self.wfile.write(json.dumps({"status": "ok", "processed": len(res), "results": res}).encode())
  def do_GET(self):
    if self.path == "/health":
      self.send_response(200)
      self.send_header("Content-Type", "application/json")
      self.end_headers()
      self.wfile.write(b'{"status":"ok"}')
    else:
      self.send_error(404)
  def log_message(self, fmt, *a): log.info(fmt % a)
def call_wm(eid, bk, et, lb):
  if not WTOKEN: return {"entity_id": eid, "windmill": "skipped"}
  payload = json.dumps({
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "runScriptByPath",
      "arguments": {
        "path": "u/admin/enrich-trigger",
        "args": {
          "entity_id": str(eid),
          "business_key": bk,
          "entity_type": et,
          "label": lb,
          "source_system": "hubspot-webhook"
        }
      }
    },
    "id": 1
  }).encode()
  try:
    req = Request(
      WURL+"/api/mcp/w/admins/mcp",
      data=payload,
      headers={"Authorization": "Bearer "+WTOKEN, "Content-Type": "application/json", "Accept": "application/json, text/event-stream"},
      method="POST"
    )
    resp = urlopen(req, timeout=60)
    data = resp.read().decode()
    for line in data.split("\\n"):
      if line.startswith("data: "):
        try:
          result = json.loads(line[6:])
          if "result" in result:
            content = result["result"].get("content", [])
            if content and isinstance(content[0], dict):
              text = content[0].get('text', '').strip(chr(34))
              try:
                wm_result = json.loads(text)
                euuid = wm_result.get("entity_id", text) if isinstance(wm_result, dict) else text
              except:
                euuid = text
              log.info("EnrichIQ UUID: %s", euuid)
              return {"entity_id": eid, "windmill": "ok", "enrichiq_uuid": euuid}
        except: continue
    return {"entity_id": eid, "windmill": "ok", "enrichiq_uuid": eid}
  except Exception as e: log.error("WM err: %s", e); return {"entity_id": eid, "windmill": str(e)}
def store_hs(ot, hid, eid):
  if not HTOKEN: return False
  ht = "contacts" if ot == "CONTACT" else "companies"
  url = f"https://api.hubapi.com/crm/v3/objects/{ht}/{hid}"
  now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
  data = json.dumps({"properties": {"entity_id": eid, "synced_crm": now}}).encode()
  try:
    urlopen(Request(url, data=data, headers={"Authorization": "Bearer "+HTOKEN, "Content-Type": "application/json"}, method="PATCH"), timeout=30)
    log.info("Stored %s %s on %s %s", eid, now, ht, hid)
    return True
  except Exception as e: log.error("HS err: %s", e); return False
HTTPServer(('0.0.0.0', PORT), H).serve_forever()

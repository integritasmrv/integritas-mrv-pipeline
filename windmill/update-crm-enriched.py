import psycopg2, json, logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger()
CRM = {
    "MRV": {"host": "10.0.13.2", "dbname": "integritasmrv_crm", "user": "integritasmrv_crm_user", "password": "REPLACE_MRV_PASSWORD"},
    "Power": {"host": "10.0.14.2", "dbname": "poweriq_crm", "user": "poweriq_crm_user", "password": "REPLACE_POWER_PASSWORD"}
}
def get_enriched(crm_key, entity_type, hubspot_id):
    cfg = CRM[crm_key]
    conn = psycopg2.connect(host=cfg["host"], dbname=cfg["dbname"], user=cfg["user"], password=cfg["password"])
    cur = conn.cursor()
    table = "nb_crm_contacts" if entity_type == "contact" else "nb_crm_customers"
    cur.execute(f"SELECT id, hubspot_id, enrichment_status, enrichment_score, last_enriched_at FROM {table} WHERE hubspot_id = %s LIMIT 1", (hubspot_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return {"id": row[0], "hubspot_id": row[1], "status": row[2], "score": row[3], "enriched_at": row[4]}
    return None
def update_hubspot(hubspot_id, data, token):
    import urllib.request
    url = f"https://api.hubapi.com/crm/v3/objects/contacts/{hubspot_id}"
    req = urllib.request.Request(url, data=json.dumps({"properties": data}).encode(), headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"}, method="PATCH")
    try:
        urllib.request.urlopen(req, timeout=30)
        return True
    except: return False
def main(hubspot_id: str = "", crm: str = "MRV", entity_type: str = "contact", hubspot_token: str = ""):
    log.info(f"update_crm_enriched: hs_id={hubspot_id}, crm={crm}, type={entity_type}")
    enriched = get_enriched(crm, entity_type, hubspot_id)
    if not enriched:
        log.warning(f"No enriched record found for {hubspot_id}")
        return {"status": "not_found"}
    props = {"enrichment_status": enriched["status"] or ""}
    if enriched.get("score"): props["enrichment_score"] = str(enriched["score"])
    if enriched.get("enriched_at"): props["last_enriched_at"] = str(enriched["enriched_at"])
    if hubspot_id and update_hubspot(hubspot_id, props, hubspot_token):
        log.info(f"Updated HubSpot {hubspot_id}")
        return {"status": "updated", "hubspot_id": hubspot_id, "props": props}
    return {"status": "failed"}

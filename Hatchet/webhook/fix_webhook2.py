#!/usr/bin/env python3
"""Fix escaped quotes in webhook handler"""
import subprocess

r = subprocess.run(['docker', 'exec', 'integritasmrv-api', 'cat', '/app/api_main.py'], capture_output=True, text=True)
content = r.stdout

start_marker = '@app.post("/webhook/hubspot")'
end_marker = '\nasync def query_rag'
start_idx = content.find(start_marker)
end_idx = content.find(end_marker, start_idx)

if start_idx == -1 or end_idx == -1:
    print(f"ERROR: start={start_idx} end={end_idx}")
    exit(1)

new_handler = """@app.post("/webhook/hubspot")
async def webhook_hubspot(request: Request):
    try:
        body = await request.json()
        events = body if isinstance(body, list) else [body]
        print(f"[WEBHOOK] HubSpot: {len(events)} event(s)", flush=True)
        results = []
        for evt in events:
            try:
                props = evt.get("properties", {})
                ot = evt.get("objectType", evt.get("subscription", {}).get("objectType", "contact"))
                hs_id = str(evt.get("objectId", evt.get("id", "")))
                ev = props.get("email", {})
                email = ev.get("value", "") if isinstance(ev, dict) else str(ev or "")
                crm_name = "integritasmrv"
                import asyncpg
                cfg = POWERIQ_CONFIG if crm_name == "poweriq" else {"host": "crm-integritasmrv-db", "port": 5432, "user": "integritasmrv_crm_user", "password": "oYxxPKRfAHAD263VSDcKmljKY0vInx2QTl6PooKoqmmiDops", "database": "integritasmrv_crm"}
                if "company" in ot.lower():
                    conn = await asyncpg.connect(**cfg)
                    nv = props.get("name", props.get("company", {}))
                    name = nv.get("value", "") if isinstance(nv, dict) else str(nv or "")
                    if not name:
                        name = "Company " + hs_id
                    ex = await conn.fetchrow("SELECT id FROM nb_crm_customers WHERE hubspot_id = $1", hs_id)
                    if ex:
                        await conn.execute("UPDATE nb_crm_customers SET enrichment_status = 'To Be Enriched', updatedAt = NOW() WHERE hubspot_id = $1", hs_id)
                        results.append({"action": "updated", "crm": crm_name, "type": "company", "id": ex["id"]})
                    else:
                        row = await conn.fetchrow("INSERT INTO nb_crm_customers (name, hubspot_id, enrichment_status, updatedAt, createdAt) VALUES ($1, $2, 'To Be Enriched', NOW(), NOW()) RETURNING id", name, hs_id)
                        results.append({"action": "created", "crm": crm_name, "type": "company", "id": row["id"] if row else None})
                    await conn.close()
                else:
                    conn = await asyncpg.connect(**cfg)
                    fv = props.get("firstname", {})
                    lv = props.get("lastname", {})
                    fn = fv.get("value", "") if isinstance(fv, dict) else str(fv or "")
                    ln = lv.get("value", "") if isinstance(lv, dict) else str(lv or "")
                    full = (fn + " " + ln).strip() or (email.split("@")[0] if email else "Unknown")
                    ex = await conn.fetchrow("SELECT id FROM nb_crm_contacts WHERE hubspot_id = $1", hs_id)
                    if ex:
                        await conn.execute("UPDATE nb_crm_contacts SET enrichment_status = 'To Be Enriched', updatedAt = NOW() WHERE hubspot_id = $1", hs_id)
                        results.append({"action": "updated", "crm": crm_name, "type": "contact", "id": ex["id"]})
                    else:
                        row = await conn.fetchrow("INSERT INTO nb_crm_contacts (name, email, hubspot_id, enrichment_status, updatedAt, createdAt) VALUES ($1, $2, $3, 'To Be Enriched', NOW(), NOW()) RETURNING id", full, email or None, hs_id)
                        results.append({"action": "created", "crm": crm_name, "type": "contact", "id": row["id"] if row else None})
                    await conn.close()
            except Exception as ee:
                print(f"[WEBHOOK] Event error: {ee}")
                results.append({"error": str(ee)[:100]})
        return {"status": "processed", "results": results}
    except Exception as e:
        print(f"HubSpot webhook error: {e}")
        return {"status": "error", "detail": str(e)}

"""

new_content = content[:start_idx] + new_handler + content[end_idx:]
with open('/tmp/api_main_fixed.py', 'w') as f:
    f.write(new_content)
print(f"Written: {len(new_content)} bytes")

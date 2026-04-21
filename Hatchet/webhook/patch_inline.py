#!/usr/bin/env python3
import sys

with open('/tmp/api_main_new.py', 'r') as f:
    lines = f.readlines()

start = end = None
for i, l in enumerate(lines):
    if '@app.post("/webhook/hubspot")' in l:
        start = i
    if start and 'async def query_rag' in l:
        end = i
        break

print(f"start={start}, end={end}")

new_handler = []
new_handler.append('\n')
new_handler.append('async def webhook_hubspot(request: Request):\n')
new_handler.append('    try:\n')
new_handler.append('        body = await request.json()\n')
new_handler.append('        events = body if isinstance(body, list) else [body]\n')
new_handler.append('        print(f"[WEBHOOK] HubSpot: {len(events)} event(s)", flush=True)\n')
new_handler.append('        results = []\n')
new_handler.append('        for evt in events:\n')
new_handler.append('            try:\n')
new_handler.append('                props = evt.get("properties", {})\n')
new_handler.append('                ot = evt.get("objectType", evt.get("subscription", {}).get("objectType", "contact"))\n')
new_handler.append('                hs_id = str(evt.get("objectId", evt.get("id", "")))\n')
new_handler.append('                ev = props.get("email", {})\n')
new_handler.append('                email = ev.get("value", "") if isinstance(ev, dict) else str(ev or "")\n')
new_handler.append('                crm_name = "integritasmrv"\n')
new_handler.append('                import asyncpg\n')
new_handler.append('                cfg = {"host": "crm-integritasmrv-db", "port": 5432, "user": "integritasmrv_crm_user", "password": "oYxxPKRfAHAD263VSDcKmljKY0vInx2QTl6PooKoqmmiDops", "database": "integritasmrv_crm"}\n')
new_handler.append('                if "company" in ot.lower():\n')
new_handler.append('                    conn = await asyncpg.connect(**cfg)\n')
new_handler.append('                    nv = props.get("name", props.get("company", {}))\n')
new_handler.append('                    name = nv.get("value", "") if isinstance(nv, dict) else str(nv or "")\n')
new_handler.append('                    if not name:\n')
new_handler.append('                        name = "Company " + hs_id\n')
new_handler.append('                    ex = await conn.fetchrow("SELECT id FROM nb_crm_customers WHERE hubspot_id = $1", hs_id)\n')
new_handler.append('                    if ex:\n')
new_handler.append('                        await conn.execute("UPDATE nb_crm_customers SET enrichment_status = \'To Be Enriched\', updatedAt = NOW() WHERE hubspot_id = $1", hs_id)\n')
new_handler.append('                        results.append({"action": "updated", "crm": crm_name, "type": "company", "id": ex["id"]})\n')
new_handler.append('                    else:\n')
new_handler.append('                        row = await conn.fetchrow("INSERT INTO nb_crm_customers (name, hubspot_id, enrichment_status, updatedAt, createdAt) VALUES ($1, $2, \'To Be Enriched\', NOW(), NOW()) RETURNING id", name, hs_id)\n')
new_handler.append('                        results.append({"action": "created", "crm": crm_name, "type": "company", "id": row["id"] if row else None})\n')
new_handler.append('                    await conn.close()\n')
new_handler.append('                else:\n')
new_handler.append('                    conn = await asyncpg.connect(**cfg)\n')
new_handler.append('                    fv = props.get("firstname", {})\n')
new_handler.append('                    lv = props.get("lastname", {})\n')
new_handler.append('                    fn2 = fv.get("value", "") if isinstance(fv, dict) else str(fv or "")\n')
new_handler.append('                    ln2 = lv.get("value", "") if isinstance(lv, dict) else str(lv or "")\n')
new_handler.append('                    full = (fn2 + " " + ln2).strip() or (email.split("@")[0] if email else "Unknown")\n')
new_handler.append('                    ex = await conn.fetchrow("SELECT id FROM nb_crm_contacts WHERE hubspot_id = $1", hs_id)\n')
new_handler.append('                    if ex:\n')
new_handler.append('                        await conn.execute("UPDATE nb_crm_contacts SET enrichment_status = \'To Be Enriched\', updatedAt = NOW() WHERE hubspot_id = $1", hs_id)\n')
new_handler.append('                        results.append({"action": "updated", "crm": crm_name, "type": "contact", "id": ex["id"]})\n')
new_handler.append('                    else:\n')
new_handler.append('                        row = await conn.fetchrow("INSERT INTO nb_crm_contacts (name, email, hubspot_id, enrichment_status, updatedAt, createdAt) VALUES ($1, $2, $3, \'To Be Enriched\', NOW(), NOW()) RETURNING id", full, email or None, hs_id)\n')
new_handler.append('                        results.append({"action": "created", "crm": crm_name, "type": "contact", "id": row["id"] if row else None})\n')
new_handler.append('                    await conn.close()\n')
new_handler.append('            except Exception as ee:\n')
new_handler.append('                print(f"[WEBHOOK] Event error: {ee}")\n')
new_handler.append('                results.append({"error": str(ee)[:100]})\n')
new_handler.append('        return {"status": "processed", "results": results}\n')
new_handler.append('    except Exception as e:\n')
new_handler.append('        print(f"HubSpot webhook error: {e}")\n')
new_handler.append('        return {"status": "error", "detail": str(e)}\n')
new_handler.append('\n')

result_lines = lines[:start] + new_handler + lines[end:]
result = ''.join(result_lines)

with open('/tmp/api_main_fixed.py', 'w') as f:
    f.write(result)

print(f"Written: {len(result)} bytes")

try:
    compile(result, '/tmp/api_main_fixed.py', 'exec')
    print("Syntax OK")
except SyntaxError as e:
    print(f"Syntax error: {e}")

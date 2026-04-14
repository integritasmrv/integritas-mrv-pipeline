import urllib.request, json, time

TOKEN = "<HUBSPOT_TOKEN>"
OWNER_ID = "33593468"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
BATCH_SIZE = 10
PAUSE_MINUTES = 10

def get_all_ids(object_type):
    all_ids = []
    after = None
    while True:
        body = {"filterGroups": [], "properties": ["id"], "limit": 100}
        if after:
            body["after"] = after
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            f"https://api.hubapi.com/crm/v3/objects/{object_type}/search",
            data=data, headers=HEADERS, method="POST"
        )
        r = urllib.request.urlopen(req, timeout=30)
        d = json.loads(r.read().decode())
        ids = [r["id"] for r in d.get("results", [])]
        all_ids.extend(ids)
        paging = d.get("paging", {})
        after = paging.get("next", {}).get("after")
        if not after:
            break
    return all_ids

def update_batch(object_type, ids):
    inputs = [{"id": bid, "properties": {"hubspot_owner_id": OWNER_ID}} for bid in ids]
    payload = json.dumps({"inputs": inputs}).encode()
    req = urllib.request.Request(
        f"https://api.hubapi.com/crm/v3/objects/{object_type}/batch/update",
        data=payload, headers=HEADERS, method="POST"
    )
    r = urllib.request.urlopen(req, timeout=60)
    result = json.loads(r.read().decode())
    return len(result.get("results", []))

for obj_type in ["contacts", "companies"]:
    print(f"\n=== {obj_type.upper()} ===")
    ids = get_all_ids(obj_type)
    total = len(ids)
    print(f"Total {obj_type}: {total}")
    
    for i in range(0, total, BATCH_SIZE):
        batch = ids[i:i+BATCH_SIZE]
        updated = update_batch(obj_type, batch)
        print(f"  Batch {i//BATCH_SIZE + 1}/{(total+BATCH_SIZE-1)//BATCH_SIZE}: updated {updated} {obj_type} ({i+updated}/{total})")
        if i + BATCH_SIZE < total:
            print(f"  Sleeping {PAUSE_MINUTES} minutes...")
            time.sleep(PAUSE_MINUTES * 60)
    
    print(f"  {obj_type} DONE")

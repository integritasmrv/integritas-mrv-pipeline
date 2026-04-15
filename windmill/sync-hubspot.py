def main(entity_id: str, hubspot_token: str = ""):
    import urllib.request, json
    h = {"Authorization": "Bearer " + hubspot_token}
    req = urllib.request.Request("http://intelligence-enrichiq:8088/api/enrichment/status/" + entity_id, headers=h, method="GET")
    try:
        r = urllib.request.urlopen(req, timeout=30)
        data = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 404: data = {"entity_id": entity_id, "status": "not_found"}
        else: raise
    sr = urllib.request.Request("https://api.hubapi.com/crm/v3/objects/contacts/search", data=json.dumps({"filterGroups": [{"filters": [{"propertyName": "entity_id", "operator": "EQ", "value": entity_id}]}]}).encode(), headers={**h, "Content-Type": "application/json"}, method="POST")
    hr = urllib.request.urlopen(sr, timeout=30)
    hd = json.loads(hr.read().decode())
    if hd.get("results"):
        cid = hd["results"][0]["id"]
        pr = urllib.request.Request("https://api.hubapi.com/crm/v3/objects/contacts/" + cid, data=json.dumps({"properties": data}).encode(), headers={**h, "Content-Type": "application/json"}, method="PATCH")
        urllib.request.urlopen(pr, timeout=30)
        return {"status": "updated", "contact_id": cid}
    else:
        po = urllib.request.Request("https://api.hubapi.com/crm/v3/objects/contacts", data=json.dumps({"properties": {**data, "entity_id": entity_id}}).encode(), headers={**h, "Content-Type": "application/json"}, method="POST")
        nr = urllib.request.urlopen(po, timeout=30)
        return {"status": "created", "contact_id": json.loads(nr.read().decode()).get("id")}

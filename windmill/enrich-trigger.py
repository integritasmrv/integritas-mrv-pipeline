def main(entity_id: str = "test", business_key: str = "integritasmrv", entity_type: str = "company", label: str = "Test Entity", source_system: str = "windmill"):
    import urllib.request, json
    payload = {
        "entity_type": entity_type,
        "label": label,
        "business_key": business_key,
        "source_system": source_system,
    }
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        "http://intelligence-enrichiq:8088/api/enrichment/trigger",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        r = urllib.request.urlopen(req, timeout=60)
        result = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise Exception(f"HTTP {e.code}: {body}")
    return result

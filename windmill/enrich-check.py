def main(entity_id: str = "test"):
    import urllib.request, json

    req = urllib.request.Request(
        f"http://intelligence-enrichiq:8088/api/enrichment/status/{entity_id}",
        headers={"Content-Type": "application/json"},
        method="GET"
    )
    try:
        r = urllib.request.urlopen(req, timeout=30)
        return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return {"status": "not_found", "entity_id": entity_id}
        raise
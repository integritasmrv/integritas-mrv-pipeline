import asyncio
import logging
from typing import Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class FreeLayerResult:
    score: float
    data: dict[str, Any]
    sources: list[str]
    errors: list[str]


async def run_requests_search(query: str) -> dict[str, Any]:
    try:
        import requests
        from bs4 import BeautifulSoup
        search_url = f"https://www.google.com/search?q={query}"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(search_url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        results = []
        for item in soup.select(".tF2Cxc")[:5]:
            results.append({
                "title": item.get_text()[:200],
                "link": item.a["href"] if item.a else None
            })
        return {"success": True, "results": results}
    except Exception as e:
        logger.error(f"requests search failed: {e}")
        return {"success": False, "error": str(e), "results": []}


async def run_spiderfoot_search(query: str, sf_module: str = "ss公开") -> dict[str, Any]:
    try:
        import requests
        sf_url = "http://localhost:5001/api/scan"
        payload = {"scan-name": f"enrichiq-{query[:50]}", "module": sf_module, "target": query}
        response = requests.post(sf_url, json=payload, timeout=30)
        if response.status_code == 200:
            return {"success": True, "scan_id": response.json().get("scan-id")}
        return {"success": False, "error": "scan failed"}
    except Exception as e:
        logger.error(f"SpiderFoot search failed: {e}")
        return {"success": False, "error": str(e)}


async def run_camofox_search(query: str) -> dict[str, Any]:
    try:
        import os
        import httpx
        camofox_url = os.getenv("CAMOFOX_URL", "http://localhost:8080")
        api_key = os.getenv("CAMOFOX_API_KEY", "")
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{camofox_url}/api/scrape",
                json={"query": query, "render": True},
                headers={"Authorization": f"Bearer {api_key}"}
            )
            if response.status_code == 200:
                return {"success": True, "data": response.json()}
            return {"success": False, "error": f"HTTP {response.status_code}"}
    except Exception as e:
        logger.error(f"camofox search failed: {e}")
        return {"success": False, "error": str(e)}


async def run_linkedin_search(query: str, session_pool: Any = None) -> dict[str, Any]:
    try:
        if session_pool is None:
            from services.linkedin_scraper import LinkedInScraper
            session_pool = LinkedInScraper()
        result = await session_pool.scrape_company(query)
        return {"success": True, "data": result}
    except Exception as e:
        logger.error(f"LinkedIn search failed: {e}")
        return {"success": False, "error": str(e)}

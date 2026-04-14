# EnrichIQ Integration Flow

## Overview

HubSpot -> CRM -> Enrichment -> Scrape -> Enrich -> write back

## Data Flow

1. HubSpot sends enrichment request
2. CRM routes to business-specific queue
3. Worker picks up from business queue
4. Free Layer runs parallel scraping
5. Score Check: If score < 0.75, escalate
6. LLM Extraction from combined data
7. HubSpot Writeback

## Per-Business Queues

integritasmrv | poweriq | airbnb | private | ev-batteries

## Free Layer

Parallel asyncio.gather(): requests(0.25) + SpiderFoot(0.30) + camofox(0.35) + LinkedIn(0.25)
Escalate to paid if score < 0.75

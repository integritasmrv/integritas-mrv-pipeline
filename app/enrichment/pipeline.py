from app.enrichment.free_layer import run_parallel_free_layer, should_escalate_to_paid
from app.enrichment.context_builder import build_enrichment_context
from app.activities.extraction import extract_fields_llm, write_hubspot
import logging

logger = logging.getLogger(__name__)

async def run_enrichment_pipeline(entity_id, query, business_key):
    free_result = await run_parallel_free_layer(query)
    if should_escalate_to_paid(free_result.score):
        logger.info("Would escalate to paid sources")
    context = build_enrichment_context(query, free_result.data)
    extracted = await extract_fields_llm(context)
    await write_hubspot(entity_id, extracted)
    return extracted

from datetime import timedelta
from temporalio import workflow


@workflow.defn
class EnrichmentWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> dict:
        from workers.activities.upsert_crm import get_crm_entity
        from workers.activities.update_hubspot import trigger_enrichiq

        entity_id = payload["entity_id"]
        crm_name = payload["crm_name"]
        hubspot_id = payload.get("hubspot_id")
        entity_type = payload.get("entity_type", "contact")

        timeout = timedelta(seconds=30)

        entity = await workflow.execute_activity(
            get_crm_entity,
            args=[crm_name, "nb_crm_contacts", entity_id],
            start_to_close_timeout=timeout,
        )

        external_ids = entity.get("external_ids", {}) if entity else {}
        business_key = external_ids.get("business_key") or hubspot_id
        if not business_key:
            return {"status": "error", "reason": "no_business_key"}

        max_rounds = 3

        for round_num in range(1, max_rounds + 1):
            trigger_result = await workflow.execute_activity(
                trigger_enrichiq,
                args=[entity_type, business_key, round_num, "hubspot"],
                start_to_close_timeout=timeout,
            )
            if trigger_result["status_code"] not in (200, 201, 202):
                return {"status": "error", "reason": "enrichiq_trigger_failed", "detail": trigger_result["body"]}

            await workflow.sleep(15)

        return {"status": "enrichment_triggered", "entity_id": entity_id, "rounds": max_rounds}

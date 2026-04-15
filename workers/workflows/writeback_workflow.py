from datetime import timedelta
from temporalio import workflow


@workflow.defn
class WritebackWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> dict:
        from workers.activities.update_crm import update_crm_enrichment
        from workers.activities.dedup_merge_check import dedup_merge_check
        from workers.activities.update_hubspot import update_hubspot_contact, update_hubspot_company

        entity_id = payload["entity_id"]
        target_crm = payload.get("target_crm", "integritasmrv")
        source_system = payload.get("source_system", "hubspot")
        entity_type = payload.get("entity_type", "contact")
        enriched_data = payload.get("enriched_data", {})
        external_ids = payload.get("external_ids", {})

        ea = enriched_data.get("entity_attributes", {})
        ea["enrichment_score"] = enriched_data.get("enrichment_score")
        ea["last_enriched_at"] = enriched_data.get("last_enriched_at")

        timeout = timedelta(seconds=30)

        update_result = await workflow.execute_activity(
            update_crm_enrichment,
            args=[entity_id, ea, "enriched", enriched_data.get("enrichment_score"), target_crm],
            start_to_close_timeout=timeout,
        )

        dedup_result = await workflow.execute_activity(
            dedup_merge_check,
            args=[entity_id, external_ids, ea, target_crm],
            start_to_close_timeout=timeout,
        )

        hubspot_result = {"updated": False}
        if source_system == "hubspot":
            if entity_type == "contact" and external_ids.get("hubspot_id"):
                hubspot_result = await workflow.execute_activity(
                    update_hubspot_contact,
                    args=[external_ids["hubspot_id"], enriched_data],
                    start_to_close_timeout=timeout,
                )
            elif entity_type == "company" and external_ids.get("hubspot_company_id"):
                hubspot_result = await workflow.execute_activity(
                    update_hubspot_company,
                    args=[external_ids["hubspot_company_id"], enriched_data],
                    start_to_close_timeout=timeout,
                )

        return {
            "crm_updated": update_result,
            "dedup": dedup_result,
            "hubspot_synced": hubspot_result,
        }

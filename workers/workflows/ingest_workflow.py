from datetime import timedelta
from temporalio import workflow


def _flatten(data: dict, prefix: str = "") -> dict:
    result = {}
    for key, value in data.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                result[f"{key}.{sub_key}"] = sub_value
        else:
            result[key] = value
    return result


@workflow.defn
class IngestWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> dict:
        from workers.activities.apply_mapping import apply_mapping
        from workers.activities.upsert_crm import upsert_crm_entity

        source = payload.get("source", "hubspot")
        mapping_name = payload.get("mapping_name", "hubspot_to_crm")
        target_crm = payload.get("target_crm", "integritasmrv")
        business_key = payload.get("business_key")

        mapped = await workflow.execute_activity(
            apply_mapping,
            args=[payload.get("data", payload), mapping_name],
            start_to_close_timeout=timedelta(seconds=30),
        )

        if isinstance(mapped, dict) and any(k in mapped for k in ("lead", "contact", "company")):
            results = {}
            for target_name, target_data in mapped.items():
                if not isinstance(target_data, dict):
                    continue
                table = target_data.pop("table", None)
                if not table:
                    continue
                non_null = {k: v for k, v in target_data.items() if v is not None}
                if not non_null:
                    continue
                target_data["enrichment_status"] = "pending"
                flat_data = _flatten(target_data)
                key_prefix = "webform" if source == "webform" else "hubspot"
                key_value = f"{key_prefix}-{target_name}-{business_key}" if business_key else None
                result = await workflow.execute_activity(
                    upsert_crm_entity,
                    args=[flat_data, target_crm, table, key_value],
                    start_to_close_timeout=timedelta(seconds=30),
                )
                results[target_name] = result

            return {
                "lead_id": results.get("lead", {}).get("id"),
                "contact_id": results.get("contact", {}).get("id"),
                "company_id": results.get("company", {}).get("id"),
                "target_crm": target_crm,
                "source": source,
                "enrichment_status": "pending",
            }

        else:
            mapped["enrichment_status"] = "pending"
            flat_data = _flatten(mapped)
            result = await workflow.execute_activity(
                upsert_crm_entity,
                args=[flat_data, target_crm, "nb_crm_contacts", business_key],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return {
                "entity_id": result["id"],
                "created_at": str(result["created"]),
                "target_crm": target_crm,
                "enrichment_status": "pending",
            }

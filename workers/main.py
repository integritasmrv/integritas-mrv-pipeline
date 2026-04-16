import asyncio
from temporalio.client import Client
from temporalio.worker import Worker, UnsandboxedWorkflowRunner

from workers.workflows.ingest_workflow import IngestWorkflow
from workers.workflows.writeback_workflow import WritebackWorkflow
from workers.workflows.enrichment_workflow import EnrichmentWorkflow
from workers.activities.apply_mapping import apply_mapping, apply_mapping_batch
from workers.activities.upsert_crm import upsert_crm_entity, get_crm_entity, check_entity_exists
from workers.activities.update_crm import update_crm_enrichment
from workers.activities.dedup_merge_check import dedup_merge_check
from workers.activities.update_hubspot import update_hubspot_contact, update_hubspot_company, trigger_enrichiq


async def main():
    temporal_addr = "10.0.4.16:7233"
    task_queue = "integritasmrv-ingest"

    client = await Client.connect(temporal_addr)

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[IngestWorkflow, WritebackWorkflow, EnrichmentWorkflow],
        activities=[
            apply_mapping,
            apply_mapping_batch,
            upsert_crm_entity,
            get_crm_entity,
            check_entity_exists,
            update_crm_enrichment,
            dedup_merge_check,
            update_hubspot_contact,
            update_hubspot_company,
            trigger_enrichiq,
        ],
        workflow_runner=UnsandboxedWorkflowRunner(),
    )

    print(f"Temporal worker connecting to {temporal_addr}, namespace=Integritasmrv, task_queue={task_queue}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

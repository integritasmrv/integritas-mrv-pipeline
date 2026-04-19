DROP TRIGGER IF EXISTS trg_notify_enrichment ON nb_crm_customers;
CREATE TRIGGER trg_notify_enrichment
  AFTER INSERT OR UPDATE OF enrichment_status ON nb_crm_customers
  FOR EACH ROW EXECUTE FUNCTION notify_enrichment_queue();

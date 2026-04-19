CREATE OR REPLACE FUNCTION notify_enrichment_queue()
RETURNS trigger AS $$
DECLARE payload JSONB;
BEGIN
  IF (TG_OP='INSERT' AND NEW.enrichment_status='To Be Enriched')
     OR (TG_OP='UPDATE' AND OLD.enrichment_status IS DISTINCT FROM NEW.enrichment_status
         AND NEW.enrichment_status='To Be Enriched') THEN
    payload := jsonb_build_object(
      'id', NEW.id,
      'name', NEW.name,
      'website', NEW.website,
      'country', NEW.country,
      'industry', NEW.industry,
      'hubspot_id', NEW.hubspot_id,
      'company_email', NEW.company_email,
      'action', lower(TG_OP)
    );
    PERFORM pg_notify('enrichment_queue', payload::text);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

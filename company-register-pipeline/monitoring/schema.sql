-- Monitoring System Schema for Pipeline Tracking
-- Compatible with PostgreSQL 15+ (postgres-vector-managed)

-- Pipeline definitions (templates) - source-agnostic
CREATE TABLE IF NOT EXISTS pipeline_definitions (
    definition_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    source_type VARCHAR(50) NOT NULL,
    run_type VARCHAR(30) DEFAULT 'initial',
    default_batch_size INT DEFAULT 50,
    config JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pipeline runs (each execution) - generic for file or API
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_name VARCHAR(100) NOT NULL,
    pipeline_version VARCHAR(20) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    run_type VARCHAR(30) NOT NULL DEFAULT 'initial',
    status VARCHAR(20) NOT NULL DEFAULT 'planned',
    progress_percent DECIMAL(5,2) DEFAULT 0,
    total_items BIGINT DEFAULT 0,
    processed_items BIGINT DEFAULT 0,
    total_batches INT DEFAULT 0,
    processed_batches INT DEFAULT 0,
    total_files INT DEFAULT 0,
    processed_files INT DEFAULT 0,
    server_load_percent DECIMAL(5,2),
    batch_size INT DEFAULT 50,
    source_config JSONB DEFAULT '{}',
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Per-dataset tracking (file, API endpoint, etc.)
CREATE TABLE IF NOT EXISTS pipeline_run_items (
    item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    item_type VARCHAR(30) NOT NULL DEFAULT 'file',
    source_path VARCHAR(500),
    table_name VARCHAR(100),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    progress_percent DECIMAL(5,2) DEFAULT 0,
    total_items BIGINT DEFAULT 0,
    processed_items BIGINT DEFAULT 0,
    pagination_state JSONB DEFAULT '{}',
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Control commands for running pipelines
CREATE TABLE IF NOT EXISTS pipeline_run_commands (
    command_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    command VARCHAR(20) NOT NULL,
    issued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    executed_at TIMESTAMP,
    executed BOOLEAN DEFAULT FALSE
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline ON pipeline_runs(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_type ON pipeline_runs(source_type, run_type);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_items_run ON pipeline_run_items(run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_commands_run ON pipeline_run_commands(run_id);

-- Functions
CREATE OR REPLACE FUNCTION update_run_progress(
    p_run_id UUID,
    p_items BIGINT DEFAULT NULL,
    p_batches INT DEFAULT NULL,
    p_files INT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE pipeline_runs SET
        processed_items = COALESCE(p_items, processed_items),
        processed_batches = COALESCE(p_batches, processed_batches),
        processed_files = COALESCE(p_files, processed_files),
        progress_percent = CASE 
            WHEN total_items > 0 THEN ROUND((processed_items::DECIMAL / total_items) * 100, 2)
            ELSE 0
        END,
        updated_at = CURRENT_TIMESTAMP
    WHERE run_id = p_run_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_run_status(
    p_run_id UUID,
    p_status VARCHAR(20),
    p_error TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE pipeline_runs SET
        status = p_status,
        error_message = p_error,
        finished_at = CASE WHEN p_status IN ('completed', 'failed', 'stopped') THEN CURRENT_TIMESTAMP ELSE NULL END,
        updated_at = CURRENT_TIMESTAMP
    WHERE run_id = p_run_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_item_progress(
    p_item_id UUID,
    p_items BIGINT DEFAULT NULL,
    p_status VARCHAR(20) DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE pipeline_run_items SET
        processed_items = COALESCE(p_items, processed_items),
        status = COALESCE(p_status, status),
        progress_percent = CASE 
            WHEN total_items > 0 THEN ROUND((processed_items::DECIMAL / total_items) * 100, 2)
            ELSE 0
        END,
        updated_at = CURRENT_TIMESTAMP
    WHERE item_id = p_item_id;
END;
$$ LANGUAGE plpgsql;

-- Default pipeline definitions
INSERT INTO pipeline_definitions (pipeline_name, description, source_type, run_type, default_batch_size)
VALUES 
    ('kbo_initial_load', 'Belgium KBO initial data load', 'KBO', 'initial', 50),
    ('kbo_merge', 'Merge KBO staging to master', 'KBO', 'merge', 100),
    ('uk_ch_initial_load', 'UK Companies House initial load', 'UK_CH', 'initial', 50),
    ('uk_ch_merge', 'Merge UK CH staging to master', 'UK_CH', 'merge', 100),
    ('global_merge', 'Merge all sources to global database', 'GLOBAL', 'merge', 200)
ON CONFLICT (pipeline_name) DO NOTHING;
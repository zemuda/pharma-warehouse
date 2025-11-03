-- Initialize PostgreSQL database for ETL metadata

-- Create schema
CREATE SCHEMA IF NOT EXISTS etl_logs;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA etl_logs TO etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA etl_logs TO etl_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA etl_logs TO etl_user;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started ON etl_logs.pipeline_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_feature ON etl_logs.pipeline_runs(feature_name);
CREATE INDEX IF NOT EXISTS idx_file_processing_status ON etl_logs.file_processing(status);
CREATE INDEX IF NOT EXISTS idx_file_processing_path ON etl_logs.file_processing(file_path);
CREATE INDEX IF NOT EXISTS idx_schema_evolution_table ON etl_logs.schema_evolution(table_name);
CREATE INDEX IF NOT EXISTS idx_dq_checks_table ON etl_logs.data_quality_checks(table_name);
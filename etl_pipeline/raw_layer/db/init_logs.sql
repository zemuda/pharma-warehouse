-- Create database
-- CREATE DATABASE pharma_warehouse;

-- Connect to the database
-- \c pharma_warehouse;

-- Create raw_layer schema
-- CREATE SCHEMA IF NOT EXISTS raw_layer;

-- Create tables in raw_layer schema
CREATE TABLE IF NOT EXISTS raw_layer.pipeline_logs (
    log_id UUID PRIMARY KEY,
    timestamp TIMESTAMP,
    level VARCHAR(10),
    logger_name VARCHAR(100),
    message TEXT,
    file_path VARCHAR(500),
    sheet_name VARCHAR(200),
    operation_type VARCHAR(50),
    duration_ms INTEGER,
    rows_processed INTEGER,
    success BOOLEAN,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS raw_layer.processing_stats (
    run_id UUID PRIMARY KEY,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    total_files INTEGER,
    total_sheets INTEGER,
    total_excel_rows INTEGER,
    total_parquet_rows INTEGER,
    total_duration_ms INTEGER,
    success_count INTEGER,
    failure_count INTEGER,
    files_with_row_mismatch INTEGER,
    total_row_difference INTEGER
);

CREATE TABLE IF NOT EXISTS raw_layer.file_watcher_logs (
    log_id UUID PRIMARY KEY,
    timestamp TIMESTAMP,
    level VARCHAR(10),
    message TEXT,
    file_path VARCHAR(500),
    event_type VARCHAR(20),
    container_name VARCHAR(100),
    success BOOLEAN,
    error_message TEXT
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_timestamp ON raw_layer.pipeline_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_level ON raw_layer.pipeline_logs(level);
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_file_path ON raw_layer.pipeline_logs(file_path);
CREATE INDEX IF NOT EXISTS idx_file_watcher_logs_timestamp ON raw_layer.file_watcher_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_file_watcher_logs_event_type ON raw_layer.file_watcher_logs(event_type);

-- Grant permissions (adjust as needed)
-- GRANT USAGE ON SCHEMA raw_layer TO postgres;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw_layer TO postgres;
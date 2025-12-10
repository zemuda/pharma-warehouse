Here's a comprehensive guide on how to run the scripts:

## Prerequisites

### 1. Install Required Packages
```bash
pip install pandas pyarrow psycopg2-binary watchdog docker python-dotenv
```

### 2. Set Up PostgreSQL

**Option A: Using Docker (Recommended)**
```bash
# Pull and run PostgreSQL
docker run --name excel-pipeline-db \
  -e POSTGRES_DB=excel_pipeline \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=password \
  -p 5432:5432 \
  -d postgres:15

# Create tables (run this once)
docker exec -i excel-pipeline-db psql -U postgres -d excel_pipeline << EOF
CREATE TABLE IF NOT EXISTS pipeline_logs (
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

CREATE TABLE IF NOT EXISTS processing_stats (
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

CREATE TABLE IF NOT EXISTS file_watcher_logs (
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
EOF
```

**Option B: Local PostgreSQL Installation**
```sql
-- Connect to PostgreSQL and run these commands
CREATE DATABASE excel_pipeline;

\c excel_pipeline;

-- Then run the same CREATE TABLE statements as above
```

### 3. Create Directory Structure
```bash
mkdir -p excel_pipeline/{data/input,data/output,logs,state}
```

### 4. Create Environment File (optional)
Create a `.env` file:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=excel_pipeline
DB_USER=postgres
DB_PASSWORD=password
WATCH_DIR=./raw_data/input
PIPELINE_CONTAINER=excel-parquet-pipeline
```

## Running the Scripts

### Method 1: Direct Pipeline Execution (One-time processing)

```bash
# Basic usage
python excel_parquet_pipeline.py \
  --input-dir ./raw_data/input \
  --output-dir ./raw_data/output \
  --state-file ./raw_data/state/processing_state.json \
  --log-level INFO

# With database configuration
python excel_parquet_pipeline.py \
  --input-dir ./raw_data/input \
  --output-dir ./raw_data/output \
  --state-file ./raw_data/state/processing_state.json \
  --db-host localhost \
  --db-port 5432 \
  --db-name excel_pipeline \
  --db-user postgres \
  --db-password password \
  --clean-orphaned \
  --show-logs \
  --log-limit 10
```

### Method 2: File Watcher (Continuous monitoring)

```bash
# Using environment variables
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=excel_pipeline
export DB_USER=postgres
export DB_PASSWORD=password
export WATCH_DIR=./raw_data/input

python file_watcher.py
```

### Method 3: Using Docker Compose (Recommended for production)

Create a `docker-compose.yml` file:

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: excel_pipeline
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 30s
      timeout: 10s
      retries: 3

  pipeline:
    build: 
      context: .
      dockerfile: Dockerfile
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      DB_HOST: postgres
      DB_PORT: 5432
      DB_NAME: excel_pipeline
      DB_USER: postgres
      DB_PASSWORD: password
      WATCH_DIR: /app/data/input
    volumes:
      - ./raw_data/input:/app/data/input
      - ./raw_data/output:/app/data/output
      - ./excel_pipeline/state:/app/state
      - ./excel_pipeline/logs:/app/logs
    command: python file_watcher.py

volumes:
  postgres_data:
```

Create a `Dockerfile`:
```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY excel_parquet_pipeline.py .
COPY file_watcher.py .

# Create necessary directories
RUN mkdir -p /app/data/input /app/data/output /app/state /app/logs

CMD ["python", "file_watcher.py"]
```

Create `requirements.txt`:
```
pandas>=1.5.0
pyarrow>=10.0.0
psycopg2-binary>=2.9.0
watchdog>=2.0.0
docker>=6.0.0
python-dotenv>=0.19.0
```

Create `init.sql` for database initialization:
```sql
CREATE TABLE IF NOT EXISTS pipeline_logs (
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

CREATE TABLE IF NOT EXISTS processing_stats (
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

CREATE TABLE IF NOT EXISTS file_watcher_logs (
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
```

Run with Docker Compose:
```bash
docker-compose up -d
```

## Testing the Setup

### 1. Test the Pipeline Directly
```bash
# Add some test Excel files to the input directory
cp /path/to/your/excel/files/*.xlsx ./raw_data/input/

# Run the pipeline
python excel_parquet_pipeline.py \
  --input-dir ./raw_data/input \
  --output-dir ./raw_data/output \
  --state-file ./raw_data/state/processing_state.json \
  --db-host localhost \
  --db-port 5432 \
  --db-name excel_pipeline \
  --db-user postgres \
  --db-password password \
  --show-logs
```

### 2. Test the File Watcher
```bash
# Start the file watcher
python file_watcher.py

# In another terminal, add a new Excel file
cp /path/to/new/excel/file.xlsx ./raw_data/input/

# The file watcher should detect and process the file automatically
```

### 3. Check Database Logs
```bash
# Connect to PostgreSQL
psql -h localhost -U postgres -d excel_pipeline

# Check logs
SELECT * FROM pipeline_logs ORDER BY timestamp DESC LIMIT 5;
SELECT * FROM file_watcher_logs ORDER BY timestamp DESC LIMIT 5;
SELECT * FROM processing_stats ORDER BY end_time DESC LIMIT 5;
```

## Useful Commands

### Monitor the System
```bash
# Check if processes are running
ps aux | grep -E "(python.*(pipeline|watcher))"

# Check Docker containers (if using Docker)
docker ps

# Check logs
tail -f excel_parquet_pipeline.log
```

### Database Maintenance
```bash
# Backup database
pg_dump -h localhost -U postgres excel_pipeline > backup.sql

# Restore database
psql -h localhost -U postgres excel_pipeline < backup.sql
```

### Clean Up
```bash
# Stop all services
docker-compose down

# Remove volumes (careful - deletes data)
docker-compose down -v

# Remove local files
rm -rf excel_pipeline/data/output/*
rm -f excel_pipeline/raw_data/state/processing_state.json
rm -f *.log
```

## Troubleshooting

### Common Issues and Solutions

1. **Database Connection Failed**
   ```bash
   # Test database connection
   psql -h localhost -U postgres -d excel_pipeline -c "SELECT 1;"
   ```

2. **Permission Issues**
   ```bash
   # Ensure directories are writable
   chmod -R 755 excel_pipeline/
   ```

3. **Missing Dependencies**
   ```bash
   # Reinstall requirements
   pip install -r requirements.txt --force-reinstall
   ```

4. **File Watcher Not Detecting Files**
   ```bash
   # Check if the directory exists and is readable
   ls -la ./raw_data/input/
   ```

This setup provides a robust Excel to Parquet processing pipeline with PostgreSQL logging and file monitoring capabilities.
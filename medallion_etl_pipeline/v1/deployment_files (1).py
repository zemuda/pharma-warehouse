# requirements.txt
"""
Python dependencies for the Medallion Architecture Pipeline
Compatible with Python 3.11, 3.12, 3.13
"""

# PySpark and Delta Lake
pyspark==3.5.0
delta-spark==3.0.0

# Prefect orchestration
prefect==2.14.21
prefect-dask==0.2.7

# Database connectivity
sqlalchemy==2.0.25
psycopg2-binary==2.9.9
pandas==2.2.0  # Compatible with Python 3.13

# Excel processing
openpyxl==3.1.2
xlrd==2.0.1

# Data quality and validation (optional - can be heavy)
# great-expectations==0.18.12

# Utilities
python-dotenv==1.0.0
pyyaml==6.0.1

# Additional dependencies for Python 3.13
numpy>=1.26.0  # Required for pandas 2.2.0


# =================================================================
# setup_environment.sh (Linux/Mac)
"""
#!/bin/bash

echo "Setting up Medallion Architecture Pipeline environment..."

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# Create directory structure
mkdir -p C:/pharma_warehouse/medallion_etl_pipeline/data/input
mkdir -p C:/pharma_warehouse/medallion_etl_pipeline/data/lakehouse/{bronze,silver,gold}
mkdir -p C:/pharma_warehouse/medallion_etl_pipeline/checkpoints
mkdir -p C:/pharma_warehouse/medallion_etl_pipeline/temp
mkdir -p C:/pharma_warehouse/medallion_etl_pipeline/logs

# Set environment variables
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=etl_metadata
export POSTGRES_USER=etl_user
export POSTGRES_PASSWORD=etl_password

echo "Environment setup complete!"
echo "To activate: source venv/bin/activate"
"""

# =================================================================
# setup_environment.bat (Windows)
"""
@echo off
echo Setting up Medallion Architecture Pipeline environment...

REM Create virtual environment
python -m venv venv
call venv\Scripts\activate.bat

REM Upgrade pip
python -m pip install --upgrade pip

REM Install dependencies
pip install -r requirements.txt

REM Create directory structure
mkdir C:\pharma_warehouse\medallion_etl_pipeline\data\input 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\data\lakehouse\bronze 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\data\lakehouse\silver 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\data\lakehouse\gold 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\checkpoints 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\temp 2>nul
mkdir C:\pharma_warehouse\medallion_etl_pipeline\logs 2>nul

REM Set environment variables
setx POSTGRES_HOST localhost
setx POSTGRES_PORT 5432
setx POSTGRES_DB etl_metadata
setx POSTGRES_USER etl_user
setx POSTGRES_PASSWORD etl_password

echo Environment setup complete!
echo To activate: venv\Scripts\activate.bat
"""

# =================================================================
# docker-compose.yml
"""
version: '3.8'

services:
  postgres:
    image: postgres:15
    container_name: etl_metadata_db
    environment:
      POSTGRES_DB: etl_metadata
      POSTGRES_USER: etl_user
      POSTGRES_PASSWORD: etl_password
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init_db.sql:/docker-entrypoint-initdb.d/init_db.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U etl_user -d etl_metadata"]
      interval: 10s
      timeout: 5s
      retries: 5

  prefect-server:
    image: prefecthq/prefect:2.14.0-python3.11
    container_name: prefect_server
    command: prefect server start
    environment:
      PREFECT_API_URL: http://prefect-server:4200/api
      PREFECT_SERVER_API_HOST: 0.0.0.0
    ports:
      - "4200:4200"
    depends_on:
      - postgres
    volumes:
      - prefect_data:/root/.prefect

volumes:
  postgres_data:
  prefect_data:
"""

# =================================================================
# init_db.sql
"""
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
"""

# =================================================================
# .env.example
"""
# PostgreSQL Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=etl_metadata
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=etl_password

# Spark Configuration
SPARK_MASTER=local[*]
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g

# Pipeline Paths
ETL_INPUT_PATH=C:/pharma_warehouse/medallion_etl_pipeline/data/input
ETL_OUTPUT_PATH=C:/pharma_warehouse/medallion_etl_pipeline/data/lakehouse
ETL_CHECKPOINT_PATH=C:/pharma_warehouse/medallion_etl_pipeline/checkpoints
ETL_TEMP_PATH=C:/pharma_warehouse/medallion_etl_pipeline/temp

# Prefect Configuration
PREFECT_API_URL=http://localhost:4200/api
PREFECT_LOGGING_LEVEL=INFO

# Feature Flags
ENABLE_SCHEMA_EVOLUTION=true
ENABLE_DATA_QUALITY_CHECKS=true
ENABLE_SCD2=true
"""

# =================================================================
# run_pipeline.py
"""
Main entry point to run the pipeline
"""

import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('C:/pharma_warehouse/medallion_etl_pipeline/logs/pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Import pipeline
from main_pipeline import medallion_pipeline

def main():
    """Run the complete pipeline"""
    try:
        logger.info("="*60)
        logger.info("Starting Medallion Architecture ETL Pipeline")
        logger.info("="*60)
        
        # Run pipeline for all features
        result = medallion_pipeline()
        
        logger.info("="*60)
        logger.info("Pipeline completed successfully!")
        logger.info(f"Results: {result}")
        logger.info("="*60)
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        return 1


def run_specific_features(feature_names: list):
    """Run pipeline for specific features"""
    try:
        logger.info(f"Running pipeline for features: {feature_names}")
        result = medallion_pipeline(feature_names=feature_names)
        logger.info(f"Pipeline completed: {result}")
        return 0
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Run Medallion Architecture Pipeline')
    parser.add_argument('--features', nargs='+', help='Specific features to process')
    parser.add_argument('--bronze-only', action='store_true', help='Run Bronze layer only')
    parser.add_argument('--silver-only', action='store_true', help='Run Silver layer only')
    parser.add_argument('--gold-only', action='store_true', help='Run Gold layer only')
    
    args = parser.parse_args()
    
    if args.features:
        exit_code = run_specific_features(args.features)
    else:
        exit_code = main()
    
    sys.exit(exit_code)


# =================================================================
# deploy_to_prefect.py
"""
Deploy pipeline to Prefect Cloud/Server
"""

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from main_pipeline import medallion_pipeline

def deploy():
    """Deploy pipeline to Prefect"""
    
    # Create deployment
    deployment = Deployment.build_from_flow(
        flow=medallion_pipeline,
        name="medallion-etl-production",
        version="1.0.0",
        work_queue_name="etl-queue",
        schedule=CronSchedule(cron="0 2 * * *", timezone="UTC"),  # Run daily at 2 AM
        parameters={},
        tags=["etl", "medallion", "production"]
    )
    
    # Apply deployment
    deployment.apply()
    
    print("✅ Deployment created successfully!")
    print("📋 Deployment name: medallion-etl-production")
    print("⏰ Schedule: Daily at 2 AM UTC")
    print("🔧 To run manually: prefect deployment run 'Medallion Architecture ETL Pipeline/medallion-etl-production'")


if __name__ == "__main__":
    deploy()


# =================================================================
# monitor_pipeline.py
"""
Monitor pipeline execution and data quality
"""

import pandas as pd
from sqlalchemy import create_engine
from config import config

def get_pipeline_runs(limit: int = 10) -> pd.DataFrame:
    """Get recent pipeline runs"""
    engine = create_engine(config.database.sqlalchemy_url)
    
    query = f"""
    SELECT 
        run_id,
        pipeline_name,
        layer,
        feature_name,
        started_at,
        completed_at,
        status,
        records_processed,
        records_valid,
        records_invalid,
        files_processed,
        EXTRACT(EPOCH FROM (completed_at - started_at)) / 60 as duration_minutes
    FROM etl_logs.pipeline_runs
    ORDER BY started_at DESC
    LIMIT {limit}
    """
    
    return pd.read_sql(query, engine)


def get_data_quality_summary() -> pd.DataFrame:
    """Get data quality check summary"""
    engine = create_engine(config.database.sqlalchemy_url)
    
    query = """
    SELECT 
        layer,
        table_name,
        check_type,
        check_status,
        COUNT(*) as check_count,
        SUM(records_affected) as total_records_affected
    FROM etl_logs.data_quality_checks
    GROUP BY layer, table_name, check_type, check_status
    ORDER BY total_records_affected DESC
    """
    
    return pd.read_sql(query, engine)


def get_scd2_statistics() -> pd.DataFrame:
    """Get SCD2 operation statistics"""
    engine = create_engine(config.database.sqlalchemy_url)
    
    query = """
    SELECT 
        table_name,
        operation_type,
        SUM(records_inserted) as total_inserted,
        SUM(records_updated) as total_updated,
        SUM(records_expired) as total_expired,
        COUNT(*) as operation_count
    FROM etl_logs.scd2_history
    GROUP BY table_name, operation_type
    ORDER BY total_inserted + total_updated + total_expired DESC
    """
    
    return pd.read_sql(query, engine)


def print_pipeline_summary():
    """Print comprehensive pipeline summary"""
    print("\n" + "="*80)
    print("📊 PIPELINE EXECUTION SUMMARY")
    print("="*80)
    
    # Recent runs
    print("\n🏃 Recent Pipeline Runs:")
    runs_df = get_pipeline_runs()
    print(runs_df.to_string(index=False))
    
    # Data quality
    print("\n✅ Data Quality Summary:")
    dq_df = get_data_quality_summary()
    print(dq_df.to_string(index=False))
    
    # SCD2 stats
    print("\n📈 SCD2 Operations:")
    scd2_df = get_scd2_statistics()
    print(scd2_df.to_string(index=False))
    
    print("\n" + "="*80)


if __name__ == "__main__":
    print_pipeline_summary()

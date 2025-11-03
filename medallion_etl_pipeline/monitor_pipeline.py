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
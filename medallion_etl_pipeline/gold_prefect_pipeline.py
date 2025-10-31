# main_pipeline.py
"""
Complete Medallion Architecture Pipeline with Prefect Orchestration
"""
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from typing import Dict, List, Optional
import logging
from datetime import datetime

from config import PipelineConfig, config
from metadata_logger import MetadataLogger
from spark_utils import SparkSessionManager
from bronze_processor import BronzeLayerProcessor
from silver_processor import SilverLayerProcessor


class GoldLayerProcessor:
    """Processes Silver data into business-ready Gold layer"""
    
    def __init__(self, spark: SparkSession, config: PipelineConfig,
                 metadata_logger: MetadataLogger):
        self.spark = spark
        self.config = config
        self.metadata_logger = metadata_logger
        self.logger = logging.getLogger(__name__)
    
    def create_aggregations(self, run_id: str) -> Dict:
        """Create business aggregations and metrics"""
        self.logger.info("Creating Gold layer aggregations")
        
        stats = {
            'tables_created': 0,
            'errors': []
        }
        
        try:
            # Daily sales summary
            self._create_daily_sales_summary(run_id)
            stats['tables_created'] += 1
            
            # Monthly sales summary
            self._create_monthly_sales_summary(run_id)
            stats['tables_created'] += 1
            
            # Customer analytics
            self._create_customer_analytics(run_id)
            stats['tables_created'] += 1
            
            # Product analytics
            self._create_product_analytics(run_id)
            stats['tables_created'] += 1
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error creating Gold aggregations: {e}")
            stats['errors'].append({'operation': 'aggregations', 'error': str(e)})
            return stats
    
    def _create_daily_sales_summary(self, run_id: str):
        """Create daily sales summary"""
        silver_path = self.config.paths.silver_path
        
        # Read all invoice tables
        invoice_tables = self._find_silver_tables("*_invoices")
        
        if not invoice_tables:
            self.logger.warning("No invoice tables found")
            return
        
        # Union all invoice data
        all_invoices = None
        for table in invoice_tables:
            try:
                df = self.spark.read.format("delta").load(f"{silver_path}/{table}")
                df = df.filter(F.col("_scd2_is_current") == True)  # Current records only
                
                if all_invoices is None:
                    all_invoices = df
                else:
                    all_invoices = all_invoices.union(df)
            except Exception as e:
                self.logger.warning(f"Could not read {table}: {e}")
        
        if all_invoices is None:
            return
        
        # Create daily aggregation
        daily_summary = all_invoices.groupBy(
            "transaction_date",
            "transaction_year", 
            "transaction_month",
            F.coalesce("branch_name", F.lit("Unknown")).alias("branch_name"),
            F.coalesce("branch_code", F.lit(0)).alias("branch_code")
        ).agg(
            F.count("*").alias("invoice_count"),
            F.sum(F.coalesce("amount_numeric", "amount", F.lit(0))).alias("total_sales"),
            F.sum(F.coalesce("vat_amount", F.lit(0))).alias("total_vat"),
            F.sum(F.coalesce("discount_amount", F.lit(0))).alias("total_discount"),
            F.countDistinct(F.coalesce("customer_code", "CUS_CODE")).alias("unique_customers"),
            F.avg(F.coalesce("amount_numeric", "amount")).alias("average_invoice_value"),
            F.min(F.coalesce("amount_numeric", "amount")).alias("min_invoice_value"),
            F.max(F.coalesce("amount_numeric", "amount")).alias("max_invoice_value")
        ).withColumn("gold_load_timestamp", F.current_timestamp()) \
         .withColumn("gold_run_id", F.lit(run_id))
        
        # Write to Gold
        gold_path = self.config.paths.get_gold_table_path("daily_sales_summary")
        daily_summary.write.format("delta") \
                    .mode("append") \
                    .partitionBy("transaction_year", "transaction_month") \
                    .save(gold_path)
        
        record_count = daily_summary.count()
        self.logger.info(f"Created daily sales summary: {record_count} records")
        
        # Log statistics
        self.metadata_logger.log_table_statistics(
            run_id=run_id,
            layer='gold',
            table_name='daily_sales_summary',
            record_count=record_count,
            file_size_mb=0.0  # Calculate if needed
        )
    
    def _create_monthly_sales_summary(self, run_id: str):
        """Create monthly sales summary from daily"""
        try:
            daily_path = self.config.paths.get_gold_table_path("daily_sales_summary")
            daily_df = self.spark.read.format("delta").load(daily_path)
            
            monthly_summary = daily_df.groupBy(
                "transaction_year",
                "transaction_month",
                "branch_name",
                "branch_code"
            ).agg(
                F.sum("invoice_count").alias("total_invoices"),
                F.sum("total_sales").alias("total_sales"),
                F.sum("total_vat").alias("total_vat"),
                F.sum("total_discount").alias("total_discount"),
                F.sum("unique_customers").alias("total_unique_customers"),
                F.avg("average_invoice_value").alias("avg_daily_invoice_value"),
                F.count("transaction_date").alias("trading_days")
            ).withColumn("gold_load_timestamp", F.current_timestamp()) \
             .withColumn("gold_run_id", F.lit(run_id))
            
            gold_path = self.config.paths.get_gold_table_path("monthly_sales_summary")
            monthly_summary.write.format("delta") \
                          .mode("append") \
                          .partitionBy("transaction_year", "transaction_month") \
                          .save(gold_path)
            
            self.logger.info(f"Created monthly sales summary: {monthly_summary.count()} records")
        except Exception as e:
            self.logger.error(f"Error creating monthly summary: {e}")
    
    def _create_customer_analytics(self, run_id: str):
        """Create customer analytics"""
        silver_path = self.config.paths.silver_path
        invoice_tables = self._find_silver_tables("*_invoices")
        
        if not invoice_tables:
            return
        
        # Union all invoice data
        all_invoices = None
        for table in invoice_tables:
            try:
                df = self.spark.read.format("delta").load(f"{silver_path}/{table}")
                df = df.filter(F.col("_scd2_is_current") == True)
                
                if all_invoices is None:
                    all_invoices = df
                else:
                    all_invoices = all_invoices.union(df)
            except:
                continue
        
        if all_invoices is None:
            return
        
        # Customer aggregation
        customer_analytics = all_invoices.groupBy(
            F.coalesce("customer_code", "CUS_CODE").alias("customer_code"),
            F.coalesce("customer_name", "CUSTOMER_NAME").alias("customer_name")
        ).agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.coalesce("amount_numeric", "amount", F.lit(0))).alias("lifetime_value"),
            F.avg(F.coalesce("amount_numeric", "amount")).alias("average_transaction_value"),
            F.min("transaction_date").alias("first_transaction_date"),
            F.max("transaction_date").alias("last_transaction_date"),
            F.countDistinct("branch_code").alias("branches_visited"),
            F.sum(F.coalesce("discount_amount", F.lit(0))).alias("total_discounts_received")
        ).withColumn("gold_load_timestamp", F.current_timestamp()) \
         .withColumn("gold_run_id", F.lit(run_id)) \
         .withColumn("customer_segment", 
                    F.when(F.col("lifetime_value") > 100000, "High Value")
                     .when(F.col("lifetime_value") > 50000, "Medium Value")
                     .otherwise("Low Value"))
        
        gold_path = self.config.paths.get_gold_table_path("customer_analytics")
        customer_analytics.write.format("delta").mode("overwrite").save(gold_path)
        
        self.logger.info(f"Created customer analytics: {customer_analytics.count()} customers")
    
    def _create_product_analytics(self, run_id: str):
        """Create product/item analytics"""
        silver_path = self.config.paths.silver_path
        
        # Find detailed invoice tables (with item-level data)
        detailed_tables = self._find_silver_tables("*detailed*_invoices")
        
        if not detailed_tables:
            self.logger.warning("No detailed tables found for product analytics")
            return
        
        all_items = None
        for table in detailed_tables:
            try:
                df = self.spark.read.format("delta").load(f"{silver_path}/{table}")
                df = df.filter(F.col("_scd2_is_current") == True)
                
                if all_items is None:
                    all_items = df
                else:
                    all_items = all_items.union(df)
            except:
                continue
        
        if all_items is None:
            return
        
        # Product aggregation
        product_analytics = all_items.groupBy(
            F.coalesce("product_code", "ITEM_CODE").alias("product_code"),
            F.coalesce("product_description", "DESCRIPTION").alias("product_description")
        ).agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.coalesce("line_quantity", "QUANTITY", F.lit(0))).alias("total_quantity_sold"),
            F.sum(F.coalesce("amount_numeric", "amount", F.lit(0))).alias("total_revenue"),
            F.avg(F.coalesce("product_unit_price", "ITEM_PRICE")).alias("average_price"),
            F.countDistinct(F.coalesce("customer_code", "CUS_CODE")).alias("unique_customers"),
            F.countDistinct("branch_code").alias("branches_sold_at")
        ).withColumn("gold_load_timestamp", F.current_timestamp()) \
         .withColumn("gold_run_id", F.lit(run_id))
        
        gold_path = self.config.paths.get_gold_table_path("product_analytics")
        product_analytics.write.format("delta").mode("overwrite").save(gold_path)
        
        self.logger.info(f"Created product analytics: {product_analytics.count()} products")
    
    def _find_silver_tables(self, pattern: str) -> List[str]:
        """Find Silver tables matching pattern"""
        from pathlib import Path
        import fnmatch
        
        silver_path = Path(self.config.paths.silver_path)
        if not silver_path.exists():
            return []
        
        tables = []
        for item in silver_path.iterdir():
            if item.is_dir() and fnmatch.fnmatch(item.name, pattern):
                tables.append(item.name)
        
        return tables


# ==================== PREFECT TASKS ====================

@task(name="Initialize Spark Session", retries=2)
def initialize_spark(config: PipelineConfig) -> bool:
    """Initialize Spark session"""
    logger = get_run_logger()
    try:
        logger.info("Initializing Spark session with Delta Lake")
        SparkSessionManager(config.spark.spark_configs)
        logger.info("Spark session initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize Spark: {e}")
        raise


@task(name="Initialize Metadata Logger")
def initialize_metadata_logger(config: PipelineConfig) -> MetadataLogger:
    """Initialize PostgreSQL metadata logger"""
    logger = get_run_logger()
    try:
        logger.info("Connecting to PostgreSQL metadata database")
        metadata_logger = MetadataLogger(config.database.sqlalchemy_url)
        logger.info("Metadata logger initialized successfully")
        return metadata_logger
    except Exception as e:
        logger.error(f"Failed to initialize metadata logger: {e}")
        raise


@task(name="Process Bronze Layer", retries=2, retry_delay_seconds=60)
def process_bronze_layer(feature_name: str, config: PipelineConfig,
                        metadata_logger: MetadataLogger) -> Dict:
    """Process Bronze layer for a feature"""
    logger = get_run_logger()
    
    # Start pipeline run
    run_id = metadata_logger.start_pipeline_run(
        pipeline_name="medallion_architecture",
        layer="bronze",
        feature_name=feature_name
    )
    
    try:
        logger.info(f"Processing Bronze layer for {feature_name}")
        
        spark = SparkSessionManager.get_instance()
        feature_config = config.features[feature_name]
        
        processor = BronzeLayerProcessor(spark, config, metadata_logger)
        stats = processor.process_feature(feature_config, run_id)
        
        # Update metadata
        metadata_logger.update_pipeline_run(
            run_id=run_id,
            status='COMPLETED',
            records_processed=stats['records_processed'],
            records_valid=stats['records_valid'],
            files_processed=stats['files_processed']
        )
        
        logger.info(f"Bronze layer completed: {stats}")
        return {'run_id': run_id, 'stats': stats}
        
    except Exception as e:
        logger.error(f"Bronze layer failed: {e}")
        metadata_logger.update_pipeline_run(
            run_id=run_id,
            status='FAILED',
            error_message=str(e)
        )
        raise


@task(name="Process Silver Layer", retries=2, retry_delay_seconds=60)
def process_silver_layer(feature_name: str, config: PipelineConfig,
                        metadata_logger: MetadataLogger,
                        bronze_run_id: str) -> Dict:
    """Process Silver layer for a feature"""
    logger = get_run_logger()
    
    run_id = metadata_logger.start_pipeline_run(
        pipeline_name="medallion_architecture",
        layer="silver",
        feature_name=feature_name,
        metadata={'bronze_run_id': bronze_run_id}
    )
    
    try:
        logger.info(f"Processing Silver layer for {feature_name}")
        
        spark = SparkSessionManager.get_instance()
        feature_config = config.features[feature_name]
        
        processor = SilverLayerProcessor(spark, config, metadata_logger)
        stats = processor.process_feature(feature_config, run_id)
        
        metadata_logger.update_pipeline_run(
            run_id=run_id,
            status='COMPLETED',
            records_processed=stats['records_processed'],
            records_valid=stats['records_valid'],
            records_invalid=stats['records_invalid']
        )
        
        logger.info(f"Silver layer completed: {stats}")
        return {'run_id': run_id, 'stats': stats}
        
    except Exception as e:
        logger.error(f"Silver layer failed: {e}")
        metadata_logger.update_pipeline_run(
            run_id=run_id,
            status='FAILED',
            error_message=str(e)
        )
        raise


@task(name="Process Gold Layer", retries=2, retry_delay_seconds=60)
def process_gold_layer(config: PipelineConfig, metadata_logger: MetadataLogger,
                      silver_run_ids: List[str]) -> Dict:
    """Process Gold layer aggregations"""
    logger = get_run_logger()
    
    run_id = metadata_logger.start_pipeline_run(
        pipeline_name="medallion_architecture",
        layer="gold",
        metadata={'silver_run_ids': silver_run_ids}
    )
    
    try:
        logger.info("Processing Gold layer")
        
        spark = SparkSessionManager.get_instance()
        processor = GoldLayerProcessor(spark, config, metadata_logger)
        stats = processor.create_aggregations(run_id)
        
        metadata_logger.update_pipeline_run(
            run_id=run_id,
            status='COMPLETED'
        )
        
        logger.info(f"Gold layer completed: {stats}")
        return {'run_id': run_id, 'stats': stats}
        
    except Exception as e:
        logger.error(f"Gold layer failed: {e}")
        metadata_logger.update_pipeline_run(
            run_id=run_id,
            status='FAILED',
            error_message=str(e)
        )
        raise


# ==================== MAIN FLOW ====================

@flow(
    name="Medallion Architecture ETL Pipeline",
    description="Complete Bronze-Silver-Gold pipeline with SCD2 and data quality",
    task_runner=ConcurrentTaskRunner()
)
def medallion_pipeline(feature_names: Optional[List[str]] = None):
    """Main ETL pipeline orchestration"""
    logger = get_run_logger()
    
    pipeline_config = config
    
    # Use all features if none specified
    if not feature_names:
        feature_names = list(pipeline_config.features.keys())
    
    logger.info(f"Starting Medallion pipeline for features: {feature_names}")
    logger.info(f"Pipeline start time: {datetime.now()}")
    
    # Initialize
    initialize_spark(pipeline_config)
    metadata_logger = initialize_metadata_logger(pipeline_config)
    
    try:
        # Process Bronze layer for all features (can run in parallel)
        bronze_results = {}
        for feature_name in feature_names:
            result = process_bronze_layer(feature_name, pipeline_config, metadata_logger)
            bronze_results[feature_name] = result
        
        logger.info("Bronze layer processing completed for all features")
        
        # Process Silver layer for all features (can run in parallel)
        silver_results = {}
        for feature_name in feature_names:
            bronze_run_id = bronze_results[feature_name]['run_id']
            result = process_silver_layer(
                feature_name, pipeline_config, metadata_logger, bronze_run_id
            )
            silver_results[feature_name] = result
        
        logger.info("Silver layer processing completed for all features")
        
        # Process Gold layer (runs after all Silver layers complete)
        silver_run_ids = [r['run_id'] for r in silver_results.values()]
        gold_result = process_gold_layer(pipeline_config, metadata_logger, silver_run_ids)
        
        logger.info("Gold layer processing completed")
        logger.info(f"Pipeline end time: {datetime.now()}")
        logger.info("=== PIPELINE COMPLETED SUCCESSFULLY ===")
        
        return {
            'status': 'SUCCESS',
            'bronze': bronze_results,
            'silver': silver_results,
            'gold': gold_result
        }
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        # Cleanup
        metadata_logger.close()
        spark_manager = SparkSessionManager.get_instance()
        # Note: Don't stop Spark here if using in interactive mode


if __name__ == "__main__":
    # Run the pipeline
    result = medallion_pipeline()
    print("\n=== Pipeline Results ===")
    print(result)

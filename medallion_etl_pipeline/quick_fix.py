# quick_fix.py
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from config import config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def quick_pipeline():
    """Run a simplified version of the pipeline"""
    try:
        # Initialize Spark directly
        logger.info("Initializing Spark directly...")
        builder = SparkSession.builder
        
        for key, value in config.spark.spark_configs.items():
            builder = builder.config(key, value)
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Spark initialized successfully")
        
        # Test file discovery
        from bronze_processor import BronzeLayerProcessor
        from metadata_logger import MetadataLogger
        
        # Initialize metadata logger
        metadata_logger = MetadataLogger(config.database.sqlalchemy_url)
        
        # Process just one feature
        feature_config = config.features['sales_cash_invoices_detailed']
        processor = BronzeLayerProcessor(spark, config, metadata_logger)
        
        # Create a test run ID
        run_id = metadata_logger.start_pipeline_run(
            pipeline_name="quick_test",
            layer="bronze", 
            feature_name='sales_cash_invoices_detailed'
        )
        
        logger.info(f"Starting Bronze processing for {feature_config.name}")
        stats = processor.process_feature(feature_config, run_id)
        
        logger.info(f"✅ Processing completed: {stats}")
        
        spark.stop()
        metadata_logger.close()
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    quick_pipeline()
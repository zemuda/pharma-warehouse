# run_simple_pipeline.py
"""
Simplified pipeline runner without PostgreSQL or Prefect
Just runs the core ETL logic with file logging
"""
import sys
import logging
from datetime import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('C:/pharma_warehouse/medallion_etl_pipeline/logs/simple_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Import core components
from config import config
from spark_utils import SparkSessionManager
from bronze_processor import BronzeLayerProcessor

# Create a mock metadata logger that just logs to file
class SimpleLogger:
    """Simple file-based logger (no database)"""
    
    def __init__(self):
        self.logger = logging.getLogger("SimpleLogger")
        self.processed_files = set()
    
    def start_pipeline_run(self, pipeline_name, layer, feature_name=None, metadata=None):
        run_id = f"{layer}_{feature_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger.info(f"Started run: {run_id}")
        return run_id
    
    def update_pipeline_run(self, run_id, status, **kwargs):
        self.logger.info(f"Run {run_id}: {status} - {kwargs}")
    
    def log_file_processing(self, run_id, file_path, status='SUCCESS', **kwargs):
        self.logger.info(f"File {file_path}: {status}")
        if status == 'SUCCESS':
            self.processed_files.add(file_path)
    
    def log_schema_evolution(self, *args, **kwargs):
        self.logger.info(f"Schema evolution: {kwargs}")
    
    def log_data_quality_check(self, *args, **kwargs):
        self.logger.info(f"Data quality: {kwargs}")
    
    def log_scd2_operation(self, *args, **kwargs):
        self.logger.info(f"SCD2: {kwargs}")
    
    def log_table_statistics(self, *args, **kwargs):
        self.logger.info(f"Table stats: {kwargs}")
    
    def get_processed_files(self, status='SUCCESS'):
        return list(self.processed_files)
    
    def get_file_hash(self, file_path):
        return None
    
    def close(self):
        pass


def run_bronze_layer_simple():
    """Run Bronze layer without database"""
    logger.info("="*60)
    logger.info("Starting SIMPLIFIED Bronze Layer Processing")
    logger.info("(No PostgreSQL or Prefect required)")
    logger.info("="*60)
    
    try:
        # Initialize Spark
        logger.info("Initializing Spark with Delta Lake...")
        SparkSessionManager(config.spark.spark_configs)
        spark = SparkSessionManager.get_instance()
        logger.info("✓ Spark initialized")
        
        # Create simple logger
        simple_logger = SimpleLogger()
        
        # Process each feature
        for feature_name, feature_config in config.features.items():
            logger.info(f"\nProcessing feature: {feature_name}")
            
            # Create processor
            processor = BronzeLayerProcessor(spark, config, simple_logger)
            
            # Generate run ID
            run_id = simple_logger.start_pipeline_run(
                pipeline_name="simple_bronze",
                layer="bronze",
                feature_name=feature_name
            )
            
            # Process feature
            stats = processor.process_feature(feature_config, run_id)
            
            # Log results
            logger.info(f"✓ Feature {feature_name} completed:")
            logger.info(f"  - Files processed: {stats['files_processed']}")
            logger.info(f"  - Records processed: {stats['records_processed']}")
            logger.info(f"  - Errors: {len(stats['errors'])}")
            
            if stats['errors']:
                for error in stats['errors']:
                    logger.error(f"  Error: {error}")
            
            simple_logger.update_pipeline_run(
                run_id=run_id,
                status='COMPLETED',
                files_processed=stats['files_processed'],
                records_processed=stats['records_processed']
            )
        
        logger.info("\n" + "="*60)
        logger.info("✅ Bronze layer processing completed!")
        logger.info("="*60)
        logger.info(f"Check output at: {config.paths.bronze_path}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1
    finally:
        simple_logger.close()


if __name__ == "__main__":
    # Check if input files exist
    input_path = Path(config.paths.base_input_path)
    if not input_path.exists():
        logger.error(f"Input directory does not exist: {input_path}")
        logger.info("Creating directory structure...")
        input_path.mkdir(parents=True, exist_ok=True)
        logger.info("Please copy your Excel files to the input directory")
        sys.exit(1)
    
    # Check for Excel files
    excel_files = list(input_path.rglob("*.xls*"))
    if not excel_files:
        logger.warning(f"No Excel files found in {input_path}")
        logger.info("Please copy your Excel files to the appropriate folders:")
        logger.info(f"  {input_path}/sales/cash_invoices/detailed/2025/january/")
        logger.info(f"  {input_path}/sales/cash_invoices/summarized/2025/january/")
        sys.exit(1)
    
    logger.info(f"Found {len(excel_files)} Excel files to process")
    
    # Run the pipeline
    exit_code = run_bronze_layer_simple()
    sys.exit(exit_code)
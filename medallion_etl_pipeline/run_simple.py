# run_simple.py
"""
Run pipeline without PostgreSQL - simplified version
"""
import sys
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('C:/pharma_warehouse/medallion_etl_pipeline/logs/pipeline_simple.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Run simplified pipeline without database"""
    logger.info("="*60)
    logger.info("Starting Simplified Pipeline (No Database)")
    logger.info("="*60)
    
    try:
        from config import config
        from spark_utils import SparkSessionManager
        
        # Initialize Spark
        logger.info("Initializing Spark...")
        SparkSessionManager(config.spark.spark_configs)
        spark = SparkSessionManager.get_instance()
        
        logger.info("✓ Spark initialized successfully")
        logger.info(f"Spark version: {spark.version}")
        
        # Test reading a file (if you have sample data)
        input_path = Path(config.paths.base_input_path)
        logger.info(f"Looking for files in: {input_path}")
        
        excel_files = list(input_path.rglob("*.xlsx")) + list(input_path.rglob("*.xls"))
        logger.info(f"Found {len(excel_files)} Excel files")
        
        if excel_files:
            logger.info("\nSample files:")
            for f in excel_files[:5]:
                logger.info(f"  - {f}")
        
        logger.info("\n✅ Setup validated successfully!")
        logger.info("To run full pipeline: Start PostgreSQL and run 'python run_pipeline.py'")
        
        return 0
        
    except Exception as e:
        logger.error(f"Setup validation failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
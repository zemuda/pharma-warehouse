# test_spark_simple.py
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_spark_initialization():
    """Test basic Spark initialization"""
    try:
        logger.info("Testing Spark initialization...")
        
        # Simple Spark config
        builder = SparkSession.builder \
            .appName("TestSpark") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.adaptive.enabled", "true")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Spark session created successfully")
        
        # Test file access
        test_path = r"C:\pharma_warehouse\medallion_etl_pipeline\data\input\sales\cash_invoices\detailed\2025\january"
        path_obj = Path(test_path)
        
        if path_obj.exists():
            logger.info(f"✅ Input path exists: {test_path}")
            files = list(path_obj.glob("*.xls*"))
            logger.info(f"✅ Found {len(files)} Excel files")
            
            if files:
                # Try to read one file
                try:
                    file_path = str(files[0])
                    logger.info(f"📖 Attempting to read: {file_path}")
                    
                    # Read with pandas first (simpler)
                    import pandas as pd
                    df_pandas = pd.read_excel(file_path)
                    logger.info(f"✅ Pandas read successful: {len(df_pandas)} rows")
                    
                    # Try with Spark
                    df_spark = spark.read \
                        .format("excel") \
                        .option("header", "true") \
                        .load(file_path)
                    
                    logger.info(f"✅ Spark read successful: {df_spark.count()} rows")
                    
                except Exception as e:
                    logger.error(f"❌ Error reading file: {e}")
        else:
            logger.error(f"❌ Input path does not exist: {test_path}")
        
        spark.stop()
        logger.info("✅ Test completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Spark test failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_spark_initialization()
    sys.exit(0 if success else 1)
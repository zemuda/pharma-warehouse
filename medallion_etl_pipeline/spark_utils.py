# spark_utils.py
"""
PySpark utilities and data quality framework
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip
from typing import Dict, List, Tuple, Optional, Any
import hashlib
import logging
from pathlib import Path


class SparkSessionManager:
    """Manages Spark session with Delta Lake support"""
    
    _instance = None
    
    def __init__(self, spark_config: Dict[str, str]):
        if SparkSessionManager._instance is not None:
            raise Exception("SparkSessionManager is a singleton")
        
        self.logger = logging.getLogger(__name__)
        builder = SparkSession.builder
        
        # Apply all configurations
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        
        # Configure Delta Lake
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        SparkSessionManager._instance = self
        self.logger.info("Spark session initialized with Delta Lake support")
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            raise Exception("SparkSessionManager not initialized")
        return cls._instance.spark
    
    def stop(self):
        if self.spark:
            self.spark.stop()
            SparkSessionManager._instance = None


class SchemaEvolutionHandler:
    """Handles schema evolution for Delta tables"""
    
    def __init__(self, spark: SparkSession, metadata_logger):
        self.spark = spark
        self.metadata_logger = metadata_logger
        self.logger = logging.getLogger(__name__)
    
    def merge_schemas(self, new_df: DataFrame, table_path: str, 
                     run_id: str, layer: str, table_name: str) -> DataFrame:
        """Merge new schema with existing table schema"""
        try:
            # Read existing table
            existing_df = self.spark.read.format("delta").load(table_path)
            existing_schema = existing_df.schema
            new_schema = new_df.schema
            
            # Detect changes
            changes = self._detect_schema_changes(existing_schema, new_schema)
            
            # Log changes
            for change in changes:
                self.metadata_logger.log_schema_evolution(
                    run_id=run_id,
                    layer=layer,
                    table_name=table_name,
                    change_type=change['type'],
                    column_name=change['column'],
                    old_data_type=change.get('old_type'),
                    new_data_type=change.get('new_type')
                )
            
            # Apply schema evolution
            if changes:
                self.logger.info(f"Applying {len(changes)} schema changes to {table_name}")
                new_df = self._apply_schema_evolution(new_df, existing_schema, changes)
            
            return new_df
            
        except Exception as e:
            if "Path does not exist" in str(e):
                # Table doesn't exist yet
                return new_df
            raise
    
    def _detect_schema_changes(self, old_schema: StructType, 
                               new_schema: StructType) -> List[Dict]:
        """Detect schema changes between old and new schemas"""
        changes = []
        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}
        
        # New columns
        for name, field in new_fields.items():
            if name not in old_fields:
                changes.append({
                    'type': 'ADD_COLUMN',
                    'column': name,
                    'new_type': str(field.dataType)
                })
        
        # Removed columns (log but don't remove)
        for name, field in old_fields.items():
            if name not in new_fields:
                changes.append({
                    'type': 'MISSING_COLUMN',
                    'column': name,
                    'old_type': str(field.dataType)
                })
        
        # Type changes
        for name in set(old_fields.keys()) & set(new_fields.keys()):
            if str(old_fields[name].dataType) != str(new_fields[name].dataType):
                changes.append({
                    'type': 'TYPE_CHANGE',
                    'column': name,
                    'old_type': str(old_fields[name].dataType),
                    'new_type': str(new_fields[name].dataType)
                })
        
        return changes
    
    def _apply_schema_evolution(self, df: DataFrame, target_schema: StructType,
                                changes: List[Dict]) -> DataFrame:
        """Apply schema evolution to dataframe"""
        # Add missing columns with nulls
        for change in changes:
            if change['type'] == 'MISSING_COLUMN':
                # Add column that exists in target but not in source
                col_name = change['column']
                df = df.withColumn(col_name, F.lit(None).cast(change['old_type']))
        
        # Reorder columns to match target schema
        target_columns = [f.name for f in target_schema.fields if f.name in df.columns]
        new_columns = [c for c in df.columns if c not in target_columns]
        
        return df.select(*(target_columns + new_columns))


class DataQualityChecker:
    """Comprehensive data quality checks"""
    
    def __init__(self, spark: SparkSession, metadata_logger):
        self.spark = spark
        self.metadata_logger = metadata_logger
        self.logger = logging.getLogger(__name__)
    
    def run_checks(self, df: DataFrame, run_id: str, layer: str, 
                   table_name: str, checks: List[str]) -> Tuple[DataFrame, Dict]:
        """Run all configured data quality checks"""
        results = {}
        
        for check in checks:
            if check == 'null_check':
                result = self._null_check(df, run_id, layer, table_name)
                results['null_check'] = result
            elif check == 'duplicate_check':
                result = self._duplicate_check(df, run_id, layer, table_name)
                results['duplicate_check'] = result
            elif check == 'amount_check':
                result = self._amount_check(df, run_id, layer, table_name)
                results['amount_check'] = result
            elif check == 'credit_amount_check':
                result = self._credit_amount_check(df, run_id, layer, table_name)
                results['credit_amount_check'] = result
        
        # Add data quality flags to dataframe
        df = self._add_quality_flags(df, results)
        
        return df, results
    
    def _null_check(self, df: DataFrame, run_id: str, layer: str, 
                    table_name: str) -> Dict:
        """Check for null values in critical columns"""
        critical_columns = [c for c in df.columns if any(
            key in c.lower() for key in ['date', 'amount', 'code', 'number']
        )]
        
        null_counts = {}
        for col in critical_columns:
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                null_counts[col] = null_count
                self.metadata_logger.log_data_quality_check(
                    run_id=run_id,
                    layer=layer,
                    table_name=table_name,
                    check_type='null_check',
                    column_name=col,
                    check_status='WARNING' if null_count < df.count() * 0.01 else 'FAILED',
                    records_affected=null_count,
                    check_details={'null_percentage': (null_count / df.count()) * 100}
                )
        
        return null_counts
    
    def _duplicate_check(self, df: DataFrame, run_id: str, layer: str,
                        table_name: str) -> Dict:
        """Check for potential duplicates"""
        # Define key columns based on table type
        if 'document_number' in [c.lower() for c in df.columns]:
            key_cols = [c for c in df.columns if any(
                key in c.lower() for key in ['document_number', 'date', 'customer']
            )]
        else:
            key_cols = df.columns[:5]  # Use first 5 columns as keys
        
        if not key_cols:
            return {}
        
        # Count duplicates
        window = Window.partitionBy(*key_cols)
        df_with_count = df.withColumn("dup_count", F.count("*").over(window))
        duplicate_count = df_with_count.filter(F.col("dup_count") > 1).count()
        
        if duplicate_count > 0:
            self.metadata_logger.log_data_quality_check(
                run_id=run_id,
                layer=layer,
                table_name=table_name,
                check_type='duplicate_check',
                check_status='WARNING',
                records_affected=duplicate_count,
                check_details={
                    'key_columns': key_cols,
                    'duplicate_percentage': (duplicate_count / df.count()) * 100
                }
            )
        
        return {'duplicate_count': duplicate_count, 'key_columns': key_cols}
    
    def _amount_check(self, df: DataFrame, run_id: str, layer: str,
                     table_name: str) -> Dict:
        """Check amount columns for validity"""
        amount_cols = [c for c in df.columns if 'amount' in c.lower()]
        
        invalid_counts = {}
        for col in amount_cols:
            # Check for negative amounts (in non-credit tables)
            if df.schema[col].dataType in [DoubleType(), FloatType(), DecimalType()]:
                invalid_count = df.filter(F.col(col) < 0).count()
                if invalid_count > 0:
                    invalid_counts[col] = invalid_count
                    self.metadata_logger.log_data_quality_check(
                        run_id=run_id,
                        layer=layer,
                        table_name=table_name,
                        check_type='amount_check',
                        column_name=col,
                        check_status='WARNING',
                        records_affected=invalid_count,
                        check_details={'issue': 'negative_amounts'}
                    )
        
        return invalid_counts
    
    def _credit_amount_check(self, df: DataFrame, run_id: str, layer: str,
                            table_name: str) -> Dict:
        """Check credit note amounts"""
        amount_cols = [c for c in df.columns if 'amount' in c.lower()]
        
        positive_counts = {}
        for col in amount_cols:
            if df.schema[col].dataType in [DoubleType(), FloatType(), DecimalType()]:
                # Credit notes should have negative or zero amounts
                positive_count = df.filter(F.col(col) > 0).count()
                if positive_count > 0:
                    positive_counts[col] = positive_count
                    self.metadata_logger.log_data_quality_check(
                        run_id=run_id,
                        layer=layer,
                        table_name=table_name,
                        check_type='credit_amount_check',
                        column_name=col,
                        check_status='WARNING',
                        records_affected=positive_count,
                        check_details={'issue': 'positive_amounts_in_credit_notes'}
                    )
        
        return positive_counts
    
    def _add_quality_flags(self, df: DataFrame, results: Dict) -> DataFrame:
        """Add data quality flags to dataframe"""
        # Add null check flags
        if 'null_check' in results and results['null_check']:
            for col, count in results['null_check'].items():
                df = df.withColumn(
                    f"dq_null_flag_{col}",
                    F.when(F.col(col).isNull(), 1).otherwise(0)
                )
        
        # Add duplicate flag
        if 'duplicate_check' in results:
            dup_result = results['duplicate_check']
            if dup_result.get('key_columns'):
                window = Window.partitionBy(*dup_result['key_columns'])
                df = df.withColumn("dq_duplicate_flag",
                    F.when(F.count("*").over(window) > 1, 1).otherwise(0))
        
        return df


def calculate_file_hash(file_path: str) -> str:
    """Calculate MD5 hash of file"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def add_metadata_columns(df: DataFrame, file_path: str, 
                        batch_id: str, layer: str) -> DataFrame:
    """Add standard metadata columns to dataframe"""
    return df.withColumn("_source_file_path", F.lit(file_path)) \
             .withColumn("_load_timestamp", F.current_timestamp()) \
             .withColumn("_batch_id", F.lit(batch_id)) \
             .withColumn("_layer", F.lit(layer)) \
             .withColumn("_file_hash", F.lit(calculate_file_hash(file_path)))


def add_scd2_columns(df: DataFrame) -> DataFrame:
    """Add SCD2 tracking columns"""
    return df.withColumn("_scd2_valid_from", F.current_timestamp()) \
             .withColumn("_scd2_valid_to", F.lit(None).cast(TimestampType())) \
             .withColumn("_scd2_is_current", F.lit(True)) \
             .withColumn("_scd2_version", F.lit(1))

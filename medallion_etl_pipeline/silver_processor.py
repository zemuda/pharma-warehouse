# silver_processor.py
"""
Silver layer processing - data cleaning, transformation, and SCD2
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from typing import List, Dict, Optional
import logging

from config import PipelineConfig, FeatureConfig
from metadata_logger import MetadataLogger
from spark_utils import (
    SchemaEvolutionHandler, DataQualityChecker, 
    add_scd2_columns
)


class SilverLayerProcessor:
    """Processes Bronze data into cleaned Silver layer with SCD2"""
    
    def __init__(self, spark: SparkSession, config: PipelineConfig,
                 metadata_logger: MetadataLogger):
        self.spark = spark
        self.config = config
        self.metadata_logger = metadata_logger
        self.logger = logging.getLogger(__name__)
        self.schema_handler = SchemaEvolutionHandler(spark, metadata_logger)
        self.quality_checker = DataQualityChecker(spark, metadata_logger)
    
    def process_feature(self, feature_config: FeatureConfig, run_id: str) -> Dict:
        """Process Bronze tables into Silver for a feature"""
        self.logger.info(f"Processing Silver layer for feature: {feature_config.name}")
        
        stats = {
            'tables_processed': 0,
            'records_processed': 0,
            'records_valid': 0,
            'records_invalid': 0,
            'scd2_operations': {},
            'errors': []
        }
        
        try:
            # Get all Bronze tables for this feature
            bronze_tables = self._discover_bronze_tables(feature_config)
            self.logger.info(f"Found {len(bronze_tables)} Bronze tables")
            
            for table_name in bronze_tables:
                try:
                    table_stats = self._process_table(
                        table_name, feature_config, run_id
                    )
                    stats['tables_processed'] += 1
                    stats['records_processed'] += table_stats['records_processed']
                    stats['records_valid'] += table_stats['records_valid']
                    stats['records_invalid'] += table_stats['records_invalid']
                    if 'scd2' in table_stats:
                        stats['scd2_operations'][table_name] = table_stats['scd2']
                except Exception as e:
                    self.logger.error(f"Error processing table {table_name}: {e}")
                    stats['errors'].append({'table': table_name, 'error': str(e)})
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error in Silver processing: {e}")
            stats['errors'].append({'feature': feature_config.name, 'error': str(e)})
            return stats
    
    def _discover_bronze_tables(self, feature_config: FeatureConfig) -> List[str]:
        """Discover all Bronze tables for a feature"""
        bronze_path = self.config.paths.bronze_path
        tables = []
        
        try:
            # List all directories in bronze path
            from pathlib import Path
            bronze_dir = Path(bronze_path)
            if bronze_dir.exists():
                for item in bronze_dir.iterdir():
                    if item.is_dir() and feature_config.name in item.name:
                        tables.append(item.name)
        except Exception as e:
            self.logger.warning(f"Error discovering Bronze tables: {e}")
        
        return tables
    
    def _process_table(self, bronze_table_name: str, 
                      feature_config: FeatureConfig, run_id: str) -> Dict:
        """Process a single Bronze table into Silver"""
        self.logger.info(f"Processing Bronze table: {bronze_table_name}")
        
        # Read Bronze data
        bronze_path = self.config.paths.get_bronze_table_path(bronze_table_name)
        df = self.spark.read.format("delta").load(bronze_path)
        
        initial_count = df.count()
        self.logger.info(f"Bronze records: {initial_count}")
        
        # Clean and transform data
        df = self._clean_and_transform(df, feature_config)
        
        # Run data quality checks
        if self.config.enable_data_quality_checks:
            df, quality_results = self.quality_checker.run_checks(
                df, run_id, 'silver', bronze_table_name, 
                feature_config.data_quality_checks
            )
        
        # Separate credit notes if this is a sales table
        if 'sales' in bronze_table_name.lower():
            invoices_df, credits_df = self._separate_credit_notes(df)
            
            # Process invoices
            silver_table = f"silver_{bronze_table_name.replace('bronze_', '')}_invoices"
            invoice_stats = self._write_with_scd2(
                invoices_df, silver_table, feature_config, run_id
            )
            
            # Process credit notes
            credit_table = f"silver_{bronze_table_name.replace('bronze_', '')}_credits"
            credit_stats = self._write_with_scd2(
                credits_df, credit_table, feature_config, run_id
            )
            
            return {
                'records_processed': initial_count,
                'records_valid': invoice_stats['records'] + credit_stats['records'],
                'records_invalid': 0,
                'scd2': {
                    'invoices': invoice_stats['scd2'],
                    'credits': credit_stats['scd2']
                }
            }
        else:
            # Process as single table
            silver_table = f"silver_{bronze_table_name.replace('bronze_', '')}"
            write_stats = self._write_with_scd2(
                df, silver_table, feature_config, run_id
            )
            
            return {
                'records_processed': initial_count,
                'records_valid': write_stats['records'],
                'records_invalid': 0,
                'scd2': write_stats['scd2']
            }
    
    def _clean_and_transform(self, df: DataFrame, 
                            feature_config: FeatureConfig) -> DataFrame:
        """Clean and transform data for Silver layer"""
        # Clean string columns
        string_columns = [f.name for f in df.schema.fields 
                         if isinstance(f.dataType, StringType)]
        
        for col in string_columns:
            df = df.withColumn(col, F.trim(F.col(col)))
        
        # Parse dates
        date_columns = [c for c in df.columns if 'date' in c.lower()]
        for col in date_columns:
            if df.schema[col].dataType == StringType():
                df = df.withColumn(
                    f"{col}_parsed",
                    F.coalesce(
                        F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss"),
                        F.to_timestamp(col, "yyyy-MM-dd"),
                        F.to_date(col, "yyyy-MM-dd")
                    )
                )
        
        # Extract transaction year and month if not present
        if 'trn_date' in [c.lower() for c in df.columns]:
            date_col = [c for c in df.columns if c.lower() == 'trn_date'][0]
            parsed_col = f"{date_col}_parsed"
            
            if parsed_col in df.columns:
                df = df.withColumn("transaction_year", F.year(parsed_col))
                df = df.withColumn("transaction_month", F.month(parsed_col))
                df = df.withColumn("transaction_date", F.to_date(parsed_col))
        
        # Parse amounts
        amount_columns = [c for c in df.columns if 'amount' in c.lower() or 'vat' in c.lower()]
        for col in amount_columns:
            if df.schema[col].dataType == StringType():
                df = df.withColumn(
                    f"{col}_numeric",
                    F.regexp_replace(col, "[^0-9.-]", "").cast(DoubleType())
                )
        
        # Add Silver metadata
        df = df.withColumn("silver_load_timestamp", F.current_timestamp())
        df = df.withColumn("silver_processing_date", F.current_date())
        
        return df
    
    def _separate_credit_notes(self, df: DataFrame) -> tuple:
        """Separate invoices and credit notes"""
        # Identify credit notes
        credit_condition = (
            (F.col("DOCUMENT_NUMBER").like("CN%")) |
            (F.col("amount") < 0) |
            (F.col("amount_numeric") < 0)
        )
        
        invoices_df = df.filter(~credit_condition)
        credits_df = df.filter(credit_condition)
        
        self.logger.info(f"Separated: {invoices_df.count()} invoices, {credits_df.count()} credit notes")
        
        return invoices_df, credits_df
    
    def _write_with_scd2(self, df: DataFrame, table_name: str,
                        feature_config: FeatureConfig, run_id: str) -> Dict:
        """Write DataFrame with SCD2 support"""
        table_path = self.config.paths.get_silver_table_path(table_name)
        
        if not self.config.enable_scd2:
            # Simple append without SCD2
            df.write.format("delta").mode("append").save(table_path)
            record_count = df.count()
            
            return {
                'records': record_count,
                'scd2': {'type': 'append_only', 'records': record_count}
            }
        
        # Add SCD2 columns if not present
        if "_scd2_valid_from" not in df.columns:
            df = add_scd2_columns(df)
        
        # Generate business key hash for change detection
        key_columns = self._identify_key_columns(df)
        df = df.withColumn(
            "_business_key_hash",
            F.sha2(F.concat_ws("||", *key_columns), 256)
        )
        
        # Generate value hash for change detection
        value_columns = [c for c in df.columns if not c.startswith("_")]
        df = df.withColumn(
            "_value_hash",
            F.sha2(F.concat_ws("||", *value_columns), 256)
        )
        
        try:
            if DeltaTable.isDeltaTable(self.spark, table_path):
                # Perform SCD2 merge
                scd2_stats = self._perform_scd2_merge(df, table_path, key_columns, run_id)
            else:
                # First load - create table
                df.write.format("delta").mode("overwrite").save(table_path)
                record_count = df.count()
                scd2_stats = {
                    'type': 'initial_load',
                    'records_inserted': record_count,
                    'records_updated': 0,
                    'records_expired': 0
                }
                
                self.metadata_logger.log_scd2_operation(
                    run_id=run_id,
                    table_name=table_name,
                    operation_type='initial_load',
                    records_inserted=record_count
                )
            
            return {'records': df.count(), 'scd2': scd2_stats}
            
        except Exception as e:
            self.logger.error(f"Error in SCD2 processing: {e}")
            raise
    
    def _identify_key_columns(self, df: DataFrame) -> List[str]:
        """Identify business key columns"""
        # Common key patterns
        key_patterns = [
            'document_number', 'invoice_number', 'customer_code', 
            'item_code', 'product_code', 'code'
        ]
        
        key_cols = []
        for col in df.columns:
            if any(pattern in col.lower() for pattern in key_patterns):
                key_cols.append(col)
        
        # Fallback: use first 3 non-metadata columns
        if not key_cols:
            key_cols = [c for c in df.columns if not c.startswith("_")][:3]
        
        return key_cols
    
    def _perform_scd2_merge(self, new_df: DataFrame, table_path: str,
                           key_columns: List[str], run_id: str) -> Dict:
        """Perform SCD2 merge operation"""
        delta_table = DeltaTable.forPath(self.spark, table_path)
        
        # Build merge condition
        merge_condition = " AND ".join([
            f"target.{col} = source.{col}" for col in key_columns
        ])
        merge_condition += " AND target._scd2_is_current = true"
        
        # Prepare staging dataframe with operation flags
        staged_df = new_df.alias("source")
        
        # Perform merge
        merge_builder = delta_table.alias("target").merge(
            staged_df,
            merge_condition
        )
        
        # When matched and values changed - expire old record
        merge_builder = merge_builder.whenMatchedUpdate(
            condition="target._value_hash != source._value_hash",
            set={
                "_scd2_valid_to": F.current_timestamp(),
                "_scd2_is_current": F.lit(False)
            }
        )
        
        # When not matched - insert new record
        merge_builder = merge_builder.whenNotMatchedInsertAll()
        
        # Execute merge
        merge_builder.execute()
        
        # Insert new versions of changed records
        changed_keys = (
            self.spark.read.format("delta").load(table_path)
            .filter(F.col("_scd2_is_current") == False)
            .filter(F.col("_scd2_valid_to").isNotNull())
            .select("_business_key_hash")
            .distinct()
        )
        
        new_versions = (
            new_df.join(changed_keys, "_business_key_hash", "inner")
            .withColumn("_scd2_version", F.col("_scd2_version") + 1)
        )
        
        if new_versions.count() > 0:
            new_versions.write.format("delta").mode("append").save(table_path)
        
        # Calculate statistics
        stats = {
            'type': 'scd2_merge',
            'records_inserted': new_df.count(),
            'records_updated': new_versions.count(),
            'records_expired': new_versions.count()
        }
        
        # Log SCD2 operation
        self.metadata_logger.log_scd2_operation(
            run_id=run_id,
            table_name=table_path.split('/')[-1],
            operation_type='SCD2_MERGE',
            records_inserted=stats['records_inserted'],
            records_updated=stats['records_updated'],
            records_expired=stats['records_expired']
        )
        
        self.logger.info(f"SCD2 merge completed: {stats}")
        return stats

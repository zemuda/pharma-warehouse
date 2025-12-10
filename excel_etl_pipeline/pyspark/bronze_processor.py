# bronze_processor.py
"""
Bronze layer processing - raw data ingestion from Excel files
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import logging
from datetime import datetime
import glob

from config import PipelineConfig, FeatureConfig
from metadata_logger import MetadataLogger
from spark_utils import (
    SchemaEvolutionHandler, DataQualityChecker, 
    add_metadata_columns, calculate_file_hash
)


class BronzeLayerProcessor:
    """Processes raw Excel files into Bronze Delta tables"""
    
    def __init__(self, spark: SparkSession, config: PipelineConfig, 
                 metadata_logger: MetadataLogger):
        self.spark = spark
        self.config = config
        self.metadata_logger = metadata_logger
        self.logger = logging.getLogger(__name__)
        self.schema_handler = SchemaEvolutionHandler(spark, metadata_logger)
    
    def process_feature(self, feature_config: FeatureConfig, run_id: str) -> Dict:
        """Process all files for a specific feature"""
        self.logger.info(f"Processing Bronze layer for feature: {feature_config.name}")
        
        stats = {
            'files_processed': 0,
            'records_processed': 0,
            'records_valid': 0,
            'records_invalid': 0,
            'errors': []
        }
        
        try:
            # Discover files
            files = self._discover_files(feature_config)
            self.logger.info(f"Discovered {len(files)} files for {feature_config.name}")
            
            # Get already processed files
            processed_files_dict = self._get_processed_files()
            
            # Filter to new or modified files
            files_to_process = []
            for file_path in files:
                if self._needs_processing(file_path, processed_files_dict):
                    files_to_process.append(file_path)
            
            self.logger.info(f"Processing {len(files_to_process)} new/modified files")
            
            # Process each file
            for file_path in files_to_process:
                try:
                    file_stats = self._process_file(file_path, feature_config, run_id)
                    stats['files_processed'] += 1
                    stats['records_processed'] += file_stats['records']
                    stats['records_valid'] += file_stats['records']
                except Exception as e:
                    self.logger.error(f"Error processing file {file_path}: {e}")
                    stats['errors'].append({'file': file_path, 'error': str(e)})
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error processing feature {feature_config.name}: {e}")
            stats['errors'].append({'feature': feature_config.name, 'error': str(e)})
            return stats
    
    def _discover_files(self, feature_config: FeatureConfig) -> List[str]:
        """Discover all files matching the feature patterns"""
        all_files = []
        
        for pattern in feature_config.source_patterns:
            full_pattern = str(Path(self.config.paths.base_input_path) / pattern)
            files = glob.glob(full_pattern, recursive=True)
            all_files.extend(files)
        
        # Filter Excel files
        excel_extensions = ['.xls', '.xlsx', '.xlsm', '.xlsb']
        all_files = [f for f in all_files if any(f.lower().endswith(ext) for ext in excel_extensions)]
        
        return sorted(list(set(all_files)))
    
    def _get_processed_files(self) -> Dict[str, str]:
        """Get dictionary of processed files with their hashes"""
        processed = self.metadata_logger.get_processed_files()
        result = {}
        for file_path in processed:
            file_hash = self.metadata_logger.get_file_hash(file_path)
            if file_hash:
                result[file_path] = file_hash
        return result
    
    def _needs_processing(self, file_path: str, processed_files: Dict[str, str]) -> bool:
        """Check if file needs processing"""
        if file_path not in processed_files:
            return True
        
        # Check if file has been modified
        current_hash = calculate_file_hash(file_path)
        return current_hash != processed_files.get(file_path)
    
    def _process_file(self, file_path: str, feature_config: FeatureConfig, 
                     run_id: str) -> Dict:
        """Process a single Excel file"""
        start_time = datetime.now()
        file_path_obj = Path(file_path)
        
        self.logger.info(f"Processing file: {file_path}")
        
        try:
            # Read Excel file with pandas (all sheets)
            excel_file = pd.ExcelFile(file_path)
            total_records = 0
            
            for sheet_name in excel_file.sheet_names:
                try:
                    # Read sheet
                    pandas_df = pd.read_excel(file_path, sheet_name=sheet_name)
                    
                    if pandas_df.empty:
                        self.logger.warning(f"Sheet '{sheet_name}' is empty, skipping")
                        continue
                    
                    # Clean dataframe
                    pandas_df = self._clean_dataframe(pandas_df, sheet_name)
                    
                    # Convert to Spark DataFrame
                    spark_df = self.spark.createDataFrame(pandas_df)
                    
                    # Add metadata
                    spark_df = self._add_bronze_metadata(
                        spark_df, file_path, sheet_name, run_id, feature_config
                    )
                    
                    # Extract date partitions from file path
                    spark_df = self._add_partition_columns(spark_df, file_path_obj)
                    
                    # Write to Delta table
                    table_name = self._get_table_name(feature_config, sheet_name)
                    self._write_to_delta(spark_df, table_name, run_id, feature_config)
                    
                    record_count = spark_df.count()
                    total_records += record_count
                    
                    # Log file processing
                    processing_time = int((datetime.now() - start_time).total_seconds() * 1000)
                    self.metadata_logger.log_file_processing(
                        run_id=run_id,
                        file_path=file_path,
                        sheet_name=sheet_name,
                        status='SUCCESS',
                        records_count=record_count,
                        file_size=file_path_obj.stat().st_size,
                        file_hash=calculate_file_hash(file_path),
                        processing_time_ms=processing_time
                    )
                    
                    self.logger.info(f"Processed sheet '{sheet_name}': {record_count} records")
                    
                except Exception as e:
                    self.logger.error(f"Error processing sheet '{sheet_name}': {e}")
                    processing_time = int((datetime.now() - start_time).total_seconds() * 1000)
                    self.metadata_logger.log_file_processing(
                        run_id=run_id,
                        file_path=file_path,
                        sheet_name=sheet_name,
                        status='FAILED',
                        processing_time_ms=processing_time,
                        error_message=str(e)
                    )
                    continue
            
            return {'records': total_records}
            
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {e}")
            raise
    
    def _clean_dataframe(self, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
        """Clean pandas dataframe"""
        # Remove completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        # Clean column names
        df.columns = [
            str(col).strip().replace(' ', '_').replace('.', '_').replace('-', '_')
            for col in df.columns
        ]
        
        # TODO: Check what this does in original code
        # Handle duplicate column names
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique():
            cols[cols == dup] = [f"{dup}_{i}" for i in range(sum(cols == dup))]
        df.columns = cols
        
        # Add source sheet name
        df['_source_sheet'] = sheet_name
        
        return df
    
    def _add_bronze_metadata(self, df: DataFrame, file_path: str, 
                            sheet_name: str, run_id: str,
                            feature_config: FeatureConfig) -> DataFrame:
        """Add Bronze layer metadata columns"""
        return df.withColumn("bronze_file_path", F.lit(file_path)) \
                 .withColumn("bronze_sheet_name", F.lit(sheet_name)) \
                 .withColumn("bronze_load_timestamp", F.current_timestamp()) \
                 .withColumn("bronze_run_id", F.lit(run_id)) \
                 .withColumn("bronze_feature", F.lit(feature_config.name)) \
                 .withColumn("bronze_file_hash", F.lit(calculate_file_hash(file_path)))
    
    def _add_partition_columns(self, df: DataFrame, file_path: Path) -> DataFrame:
        """Extract partition columns from file path"""
        # Try to extract year and month from path
        path_parts = file_path.parts
        
        year = None
        month = None
        
        for i, part in enumerate(path_parts):
            if part.isdigit() and len(part) == 4:  # Year
                year = int(part)
            elif i > 0 and path_parts[i-1].isdigit() and len(path_parts[i-1]) == 4:
                # Month name after year
                month_map = {
                    'january': 1, 'february': 2, 'march': 3, 'april': 4,
                    'may': 5, 'june': 6, 'july': 7, 'august': 8,
                    'september': 9, 'october': 10, 'november': 11, 'december': 12
                }
                month = month_map.get(part.lower(), None)
        
        # Add as columns
        df = df.withColumn("partition_year", F.lit(year))
        df = df.withColumn("partition_month", F.lit(month))
        
        return df
    
    def _get_table_name(self, feature_config: FeatureConfig, sheet_name: str) -> str:
        """Generate Bronze table name"""
        # Sanitize sheet name
        safe_sheet = sheet_name.lower().replace(' ', '_').replace('-', '_')
        return f"bronze_{feature_config.name}_{safe_sheet}"
    
    def _write_to_delta(self, df: DataFrame, table_name: str, 
                       run_id: str, feature_config: FeatureConfig):
        """Write DataFrame to Delta table with schema evolution"""
        table_path = self.config.paths.get_bronze_table_path(table_name)
        
        try:
            # Check if table exists
            if DeltaTable.isDeltaTable(self.spark, table_path):
                # Table exists - handle schema evolution
                if self.config.enable_schema_evolution:
                    df = self.schema_handler.merge_schemas(
                        df, table_path, run_id, 'bronze', table_name
                    )
                
                # Append data
                df.write.format("delta") \
                  .mode("append") \
                  .option("mergeSchema", "true") \
                  .save(table_path)
                
                self.logger.info(f"Appended to Delta table: {table_name}")
            else:
                # Create new table with partitioning
                partition_cols = [c for c in feature_config.partition_columns 
                                if c in df.columns or f"partition_{c}" in df.columns]
                
                if partition_cols:
                    df.write.format("delta") \
                      .mode("overwrite") \
                      .partitionBy(*[f"partition_{c}" for c in partition_cols if f"partition_{c}" in df.columns]) \
                      .save(table_path)
                else:
                    df.write.format("delta") \
                      .mode("overwrite") \
                      .save(table_path)
                
                self.logger.info(f"Created new Delta table: {table_name}")
            
            # Log table statistics
            record_count = self.spark.read.format("delta").load(table_path).count()
            self.metadata_logger.log_table_statistics(
                run_id=run_id,
                layer='bronze',
                table_name=table_name,
                record_count=record_count,
                file_size_mb=self._get_table_size(table_path)
            )
            
        except Exception as e:
            self.logger.error(f"Error writing to Delta table {table_name}: {e}")
            raise
    
    def _get_table_size(self, table_path: str) -> float:
        """Get table size in MB"""
        try:
            path_obj = Path(table_path)
            if path_obj.exists():
                total_size = sum(f.stat().st_size for f in path_obj.rglob('*') if f.is_file())
                return total_size / (1024 * 1024)  # Convert to MB
        except:
            pass
        return 0.0

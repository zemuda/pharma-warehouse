# bronze_loader_spark_local.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import logging
import sys
import psycopg2
from psycopg2.extras import RealDictCursor


class BronzeLayerSparkLoader:
    def __init__(
        self,
        bronze_base_path: str = "/opt/etl/bronze",
        source_data_path: str = "/opt/etl/output",
        postgres_config: Dict = None,
        spark_configs: Dict = None,
    ):
        """
        Initialize Bronze Layer Loader for local file storage

        Args:
            bronze_base_path: Local path for bronze Delta tables
            source_data_path: Local path where parquet files are stored
            postgres_config: PostgreSQL connection config
            spark_configs: Additional Spark configurations
        """
        self.bronze_base_path = Path(bronze_base_path).absolute()
        self.source_data_path = Path(source_data_path).absolute()

        # Ensure directories exist
        self.bronze_base_path.mkdir(parents=True, exist_ok=True)

        self.postgres_config = postgres_config or {
            "host": "localhost",
            "port": "5432",
            "database": "excel_pipeline",
            "user": "postgres",
            "password": "password",
            "schema": "bronze_logs",
        }

        self.features = ["sales", "supplies", "inventory"]
        self.spark_configs = spark_configs or {}

        # Initialize Spark session
        self.spark = self._create_spark_session()

        # Initialize PostgreSQL logging
        self._init_postgres_logging()

        # PostgreSQL connection pool
        self.pg_conn = None

    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session for local file storage"""
        # Default configs optimized for local storage
        default_configs = {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # Local storage optimizations
            "spark.sql.shuffle.partitions": "8",  # Reduced for local
            "spark.default.parallelism": "4",
            "spark.sql.files.maxPartitionBytes": "128MB",
            # Adaptive query execution
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
            # Memory management for local
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
            "spark.memory.fraction": "0.8",
            "spark.memory.storageFraction": "0.3",
            # Delta Lake configs
            "spark.databricks.delta.retentionDurationCheck.enabled": "false",
            "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
            "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "true",
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
            # Compression
            "spark.sql.parquet.compression.codec": "snappy",
            # Serialization
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.kryo.registrationRequired": "false",
            # Local file system
            "spark.hadoop.fs.defaultFS": "file:///",
        }

        # Merge with user configs
        default_configs.update(self.spark_configs)

        # Build Spark session
        builder = SparkSession.builder.appName("BronzeLayerLocalLoader")

        for key, value in default_configs.items():
            builder = builder.config(key, value)

        spark = builder.getOrCreate()

        # Set log level
        spark.sparkContext.setLogLevel("WARN")

        logging.info(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")

        return spark

    def _get_postgres_connection(self):
        """Get PostgreSQL connection using psycopg2 for better performance"""
        if self.pg_conn is None or self.pg_conn.closed:
            self.pg_conn = psycopg2.connect(
                host=self.postgres_config["host"],
                port=self.postgres_config["port"],
                database=self.postgres_config["database"],
                user=self.postgres_config["user"],
                password=self.postgres_config["password"],
            )
            self.pg_conn.autocommit = False
        return self.pg_conn

    def _init_postgres_logging(self):
        """Initialize PostgreSQL logging tables with proper indexes"""
        try:
            conn = self._get_postgres_connection()
            cur = conn.cursor()

            # Create schema
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.postgres_config['schema']}")

            # Load operations table
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.postgres_config['schema']}.load_operations (
                    operation_id SERIAL PRIMARY KEY,
                    feature_name VARCHAR(100) NOT NULL,
                    load_path VARCHAR(500),
                    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    completed_at TIMESTAMP,
                    files_processed INTEGER DEFAULT 0,
                    rows_loaded BIGINT DEFAULT 0,
                    status VARCHAR(50) NOT NULL,
                    error_message TEXT,
                    spark_application_id VARCHAR(100),
                    total_duration_seconds INTEGER
                )
            """
            )

            # Schema evolution table
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.postgres_config['schema']}.schema_evolution (
                    evolution_id SERIAL PRIMARY KEY,
                    feature_name VARCHAR(100) NOT NULL,
                    table_name VARCHAR(200) NOT NULL,
                    change_type VARCHAR(50) NOT NULL,
                    column_name VARCHAR(200),
                    old_data_type VARCHAR(100),
                    new_data_type VARCHAR(100),
                    change_date TIMESTAMP NOT NULL DEFAULT NOW(),
                    resolved BOOLEAN DEFAULT FALSE,
                    operation_id INTEGER REFERENCES {self.postgres_config['schema']}.load_operations(operation_id)
                )
            """
            )

            # File processing table
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.postgres_config['schema']}.file_processing (
                    file_id SERIAL PRIMARY KEY,
                    operation_id INTEGER NOT NULL REFERENCES {self.postgres_config['schema']}.load_operations(operation_id),
                    file_path VARCHAR(500) NOT NULL,
                    file_size BIGINT,
                    records_count BIGINT,
                    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    status VARCHAR(50) NOT NULL,
                    error_message TEXT,
                    data_quality_score DECIMAL(5,2),
                    processing_time_seconds DECIMAL(10,2)
                )
            """
            )

            # Delta table stats
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.postgres_config['schema']}.delta_table_stats (
                    table_name VARCHAR(200) PRIMARY KEY,
                    total_records BIGINT,
                    total_files INTEGER,
                    table_size_mb DECIMAL(10,2),
                    last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
                    partition_columns TEXT[],
                    avg_quality_score DECIMAL(5,2),
                    last_optimized TIMESTAMP
                )
            """
            )

            # Create indexes for better query performance
            indexes = [
                f"CREATE INDEX IF NOT EXISTS idx_load_ops_feature ON {self.postgres_config['schema']}.load_operations(feature_name, started_at DESC)",
                f"CREATE INDEX IF NOT EXISTS idx_load_ops_status ON {self.postgres_config['schema']}.load_operations(status)",
                f"CREATE INDEX IF NOT EXISTS idx_file_proc_operation ON {self.postgres_config['schema']}.file_processing(operation_id)",
                f"CREATE INDEX IF NOT EXISTS idx_file_proc_status ON {self.postgres_config['schema']}.file_processing(status)",
                f"CREATE INDEX IF NOT EXISTS idx_schema_evo_table ON {self.postgres_config['schema']}.schema_evolution(table_name, change_date DESC)",
            ]

            for idx_sql in indexes:
                try:
                    cur.execute(idx_sql)
                except psycopg2.Error as e:
                    logging.debug(f"Index creation note: {e}")

            conn.commit()
            logging.info("PostgreSQL logging tables initialized successfully")

        except Exception as e:
            logging.error(f"Failed to initialize PostgreSQL logging: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if cur:
                cur.close()

    def _log_to_postgres(self, table: str, data: Dict) -> Optional[int]:
        """Log data to PostgreSQL table and return ID if applicable"""
        try:
            conn = self._get_postgres_connection()
            cur = conn.cursor()

            # Build INSERT query
            # TODO: CHECK DIFFERENCE
            # placeholders = ", ".join(["%s"] * len(data))
            # columns = ", ".join(data.keys())
            # values = tuple(data.values())

            columns = list(data.keys())
            placeholders = ["%s"] * len(columns)
            values = [data[col] for col in columns]

            query = f"""
                INSERT INTO {self.postgres_config['schema']}.{table} 
                ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                RETURNING *
            """

            cur.execute(query, values)
            result = cur.fetchone()
            conn.commit()

            # Return the ID (first column is usually ID)
            return result[0] if result else None

        except Exception as e:
            logging.error(f"Failed to log to PostgreSQL {table}: {e}")
            if conn:
                conn.rollback()
            return None
        finally:
            if cur:
                cur.close()

    def _update_postgres_record(self, table: str, record_id: int, data: Dict):
        """Update PostgreSQL record"""
        try:
            conn = self._get_postgres_connection()
            cur = conn.cursor()

            # Build UPDATE query
            set_clause = ", ".join([f"{col} = %s" for col in data.keys()])
            values = list(data.values()) + [record_id]

            # Determine ID column based on table
            id_column = {
                "load_operations": "operation_id",
                "file_processing": "file_id",
                "schema_evolution": "evolution_id",
            }.get(table, "id")

            query = f"""
                UPDATE {self.postgres_config['schema']}.{table}
                SET {set_clause}
                WHERE {id_column} = %s
            """

            cur.execute(query, values)
            conn.commit()

        except Exception as e:
            logging.error(f"Failed to update PostgreSQL {table}: {e}")
            if conn:
                conn.rollback()
        finally:
            if cur:
                cur.close()

    def discover_parquet_files(self, feature: str) -> List[Path]:
        """Discover parquet files using native Python (faster for local storage)"""
        feature_path = self.source_data_path / feature

        if not feature_path.exists():
            logging.warning(f"Feature path does not exist: {feature_path}")
            return []

        try:
            # Use glob for efficient local file discovery
            parquet_files = list(feature_path.rglob("*.parquet"))
            logging.info(
                f"Found {len(parquet_files)} parquet files for feature: {feature}"
            )
            return parquet_files

        except Exception as e:
            logging.error(f"Error discovering files for {feature}: {e}")
            return []

    def get_table_name_from_path(self, file_path: Path) -> str:
        """Convert file path to table name"""
        # Get relative path from source data path
        try:
            rel_path = file_path.relative_to(self.source_data_path)
            parts = rel_path.parts[:-1]  # Exclude filename

            # Keep feature and relevant subdirectories (exclude year/month)
            # Example: sales/cash_invoices/detailed -> bronze_sales_cash_invoices_detailed
            relevant_parts = [p for p in parts if not p.isdigit()]

            table_name = "_".join(relevant_parts).lower()
            return f"bronze_{table_name}"

        except ValueError:
            # Fallback if relative path fails
            return f"bronze_{file_path.stem}"

    def _enhance_with_metadata(self, df, file_path: Path, operation_id: int):
        """Add metadata columns efficiently"""
        return (
            df.withColumn("_source_file_path", lit(str(file_path)))
            .withColumn("_source_file_name", lit(file_path.name))
            .withColumn("_load_timestamp", current_timestamp())
            .withColumn("_operation_batch_id", lit(operation_id))
            .withColumn("_ingestion_date", current_date())
            .withColumn("_spark_app_id", lit(self.spark.sparkContext.applicationId))
            .withColumn(
                "_file_modification_time",
                lit(datetime.fromtimestamp(file_path.stat().st_mtime)),
            )
            # TODO: data_hash
        )

    def _calculate_data_quality_score(self, df) -> float:
        """Calculate overall data quality score efficiently"""
        try:
            # Count nulls across all columns
            null_counts = []
            for col_name in df.columns:
                if not col_name.startswith("_"):  # Skip metadata columns
                    null_counts.append(sum(col(col_name).isNull().cast("int")))

            if not null_counts:
                return 1.0

            # Calculate quality score
            total_cells = df.count() * len(
                [c for c in df.columns if not c.startswith("_")]
            )
            total_nulls = df.agg(
                *[s.alias(f"null_{i}") for i, s in enumerate(null_counts)]
            ).collect()[0]
            null_sum = sum(total_nulls)

            quality_score = 1.0 - (null_sum / total_cells) if total_cells > 0 else 1.0
            return round(quality_score, 2)

        except Exception as e:
            logging.warning(f"Could not calculate quality score: {e}")
            return 0.0

    def _deduplicate_data(self, df, primary_keys: List[str]):
        """Remove duplicates efficiently"""
        if not primary_keys:
            return df

        try:
            # Filter out metadata columns from primary keys
            valid_pks = [pk for pk in primary_keys if pk in df.columns]

            if not valid_pks:
                return df

            window_spec = Window.partitionBy(valid_pks).orderBy(
                col("_load_timestamp").desc()
            )

            return (
                df.withColumn("_row_num", row_number().over(window_spec))
                .filter(col("_row_num") == 1)
                .drop("_row_num")
            )

        except Exception as e:
            logging.warning(f"Deduplication failed: {e}")
            return df

    def _get_primary_keys(self, table_name: str, df) -> List[str]:
        """Infer primary keys from table name and data"""
        pks = []

        # Check for common ID columns
        id_columns = [
            c for c in df.columns if c.lower().endswith("_id") or c.lower() == "id"
        ]
        if id_columns:
            pks.extend(id_columns[:2])  # Take first 2 ID columns

        # Add date column if exists
        date_columns = [c for c in df.columns if "date" in c.lower()]
        if date_columns:
            pks.append(date_columns[0])

        # Fallback: use source file path
        if not pks:
            pks = ["_source_file_path"]

        return pks

    def _write_to_delta_table(self, df, table_name: str, operation_id: int) -> bool:
        """Write DataFrame to Delta table with optimizations"""
        try:
            table_path = self.bronze_base_path / "tables" / table_name
            table_path.mkdir(parents=True, exist_ok=True)
            table_path_str = str(table_path)

            if DeltaTable.isDeltaTable(self.spark, table_path_str):
                # Merge into existing table
                delta_table = DeltaTable.forPath(self.spark, table_path_str)
                primary_keys = self._get_primary_keys(table_name, df)

                merge_condition = " AND ".join(
                    [
                        f"target.{pk} = source.{pk}"
                        for pk in primary_keys
                        if pk in df.columns
                    ]
                )

                if merge_condition:
                    (
                        delta_table.alias("target")
                        .merge(df.alias("source"), merge_condition)
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                    logging.info(f"Merged data into Delta table: {table_name}")
                else:
                    # Just append if no valid merge condition
                    df.write.format("delta").mode("append").save(table_path_str)
                    logging.info(f"Appended data to Delta table: {table_name}")

            else:
                # Create new Delta table
                partition_cols = ["_ingestion_date"]

                (
                    df.write.format("delta")
                    .mode("overwrite")
                    .partitionBy(partition_cols)
                    .option("overwriteSchema", "true")
                    .save(table_path_str)
                )
                logging.info(f"Created new Delta table: {table_name}")

            return True

        except Exception as e:
            logging.error(f"Error writing to Delta table {table_name}: {e}")
            return False

    def _optimize_delta_table(self, table_path: Path):
        """Optimize Delta table"""
        try:
            table_path_str = str(table_path)

            # Run OPTIMIZE
            self.spark.sql(f"OPTIMIZE delta.`{table_path_str}`")

            # Run VACUUM (cleanup old files)
            self.spark.sql(f"VACUUM delta.`{table_path_str}` RETAIN 168 HOURS")

            logging.info(f"Optimized Delta table: {table_path.name}")

        except Exception as e:
            logging.warning(f"Optimization failed for {table_path.name}: {e}")

    def _log_delta_table_stats(self, table_name: str, table_path: Path):
        """Log Delta table statistics"""
        try:
            table_path_str = str(table_path)

            # Get table details
            details_df = self.spark.sql(f"DESCRIBE DETAIL delta.`{table_path_str}`")
            details = details_df.collect()[0]

            # Get record count
            count_df = self.spark.sql(
                f"SELECT COUNT(*) as cnt FROM delta.`{table_path_str}`"
            )
            total_records = count_df.collect()[0]["cnt"]

            # Calculate quality score
            data_df = self.spark.read.format("delta").load(table_path_str)
            avg_quality = self._calculate_data_quality_score(data_df)

            stats_data = {
                "table_name": table_name,
                "total_records": total_records,
                "total_files": details["numFiles"],
                "table_size_mb": round(details["sizeInBytes"] / (1024**2), 2),
                "last_updated": datetime.now(),
                "partition_columns": (
                    details["partitionColumns"] if details["partitionColumns"] else []
                ),
                "avg_quality_score": avg_quality,
            }

            # Use UPSERT logic
            conn = self._get_postgres_connection()
            cur = conn.cursor()

            cur.execute(
                f"""
                INSERT INTO {self.postgres_config['schema']}.delta_table_stats 
                (table_name, total_records, total_files, table_size_mb, last_updated, 
                 partition_columns, avg_quality_score)
                VALUES (%(table_name)s, %(total_records)s, %(total_files)s, %(table_size_mb)s,
                        %(last_updated)s, %(partition_columns)s, %(avg_quality_score)s)
                ON CONFLICT (table_name) 
                DO UPDATE SET
                    total_records = EXCLUDED.total_records,
                    total_files = EXCLUDED.total_files,
                    table_size_mb = EXCLUDED.table_size_mb,
                    last_updated = EXCLUDED.last_updated,
                    partition_columns = EXCLUDED.partition_columns,
                    avg_quality_score = EXCLUDED.avg_quality_score
            """,
                stats_data,
            )

            conn.commit()
            cur.close()

        except Exception as e:
            logging.error(f"Error logging Delta table stats: {e}")

    def load_file_incrementally(
        self, file_path: Path, operation_id: int
    ) -> Tuple[bool, int]:
        """Load a single parquet file"""
        start_time = datetime.now()

        try:
            table_name = self.get_table_name_from_path(file_path)

            logging.info(f"Processing: {file_path.name} -> {table_name}")

            # Read parquet file
            df = self.spark.read.parquet(str(file_path))
            initial_count = df.count()

            # Enhance with metadata
            enhanced_df = self._enhance_with_metadata(df, file_path, operation_id)

            # Deduplicate
            primary_keys = self._get_primary_keys(table_name, enhanced_df)
            final_df = self._deduplicate_data(enhanced_df, primary_keys)
            final_count = final_df.count()

            # Calculate quality score
            quality_score = self._calculate_data_quality_score(final_df)

            # Write to Delta
            success = self._write_to_delta_table(final_df, table_name, operation_id)

            processing_time = (datetime.now() - start_time).total_seconds()

            if success:
                # Log file processing
                file_stats = {
                    "operation_id": operation_id,
                    "file_path": str(file_path),
                    "file_size": file_path.stat().st_size,
                    "records_count": final_count,
                    "processed_at": datetime.now(),
                    "status": "SUCCESS",
                    "data_quality_score": quality_score,
                    "processing_time_seconds": round(processing_time, 2),
                }
                self._log_to_postgres("file_processing", file_stats)

                # Log table stats
                table_path = self.bronze_base_path / "tables" / table_name
                self._log_delta_table_stats(table_name, table_path)

                logging.info(
                    f"✓ Processed {file_path.name}: {final_count:,} rows in {processing_time:.2f}s"
                )
                return True, final_count
            else:
                raise Exception("Failed to write to Delta table")

        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()

            # Log failure
            file_stats = {
                "operation_id": operation_id,
                "file_path": str(file_path),
                "file_size": file_path.stat().st_size if file_path.exists() else 0,
                "processed_at": datetime.now(),
                "status": "FAILED",
                "error_message": str(e)[:500],
                "processing_time_seconds": round(processing_time, 2),
            }
            self._log_to_postgres("file_processing", file_stats)

            logging.error(f"✗ Error processing {file_path.name}: {e}")
            return False, 0

    def load_feature_incrementally(self, feature: str) -> Dict:
        """Load all files for a specific feature"""
        start_time = datetime.now()

        operation_data = {
            "feature_name": feature,
            "load_path": str(self.source_data_path / feature),
            "started_at": start_time,
            "status": "STARTED",
            "spark_application_id": self.spark.sparkContext.applicationId,
        }

        # Log operation start and get ID
        operation_id = self._log_to_postgres("load_operations", operation_data)

        if operation_id is None:
            logging.error(f"Failed to log operation start for {feature}")
            return {
                "feature": feature,
                "status": "FAILED",
                "error": "Failed to log operation",
            }

        try:
            # Discover files
            all_files = self.discover_parquet_files(feature)

            if not all_files:
                logging.warning(f"No files found for feature: {feature}")
                self._update_postgres_record(
                    "load_operations",
                    operation_id,
                    {
                        "status": "COMPLETED",
                        "files_processed": 0,
                        "rows_loaded": 0,
                        "completed_at": datetime.now(),
                        "total_duration_seconds": 0,
                    },
                )
                return {"feature": feature, "status": "NO_FILES", "files_processed": 0}

            files_processed = 0
            total_rows_loaded = 0
            failed_files = []

            # Process each file
            for file_path in all_files:
                success, rows_loaded = self.load_file_incrementally(
                    file_path, operation_id
                )
                if success:
                    files_processed += 1
                    total_rows_loaded += rows_loaded
                else:
                    failed_files.append(file_path.name)

            # Update operation log
            duration = (datetime.now() - start_time).total_seconds()
            status = "COMPLETED" if not failed_files else "PARTIAL_SUCCESS"

            self._update_postgres_record(
                "load_operations",
                operation_id,
                {
                    "status": status,
                    "files_processed": files_processed,
                    "rows_loaded": total_rows_loaded,
                    "completed_at": datetime.now(),
                    "total_duration_seconds": int(duration),
                    "error_message": (
                        f"Failed files: {', '.join(failed_files)}"
                        if failed_files
                        else None
                    ),
                },
            )

            return {
                "feature": feature,
                "files_processed": files_processed,
                "total_files": len(all_files),
                "failed_files": len(failed_files),
                "total_rows_loaded": total_rows_loaded,
                "status": status,
                "duration_seconds": round(duration, 2),
            }

        except Exception as e:
            # Log operation failure
            duration = (datetime.now() - start_time).total_seconds()
            self._update_postgres_record(
                "load_operations",
                operation_id,
                {
                    "status": "FAILED",
                    "error_message": str(e)[:500],
                    "completed_at": datetime.now(),
                    "total_duration_seconds": int(duration),
                },
            )

            return {"feature": feature, "error": str(e), "status": "FAILED"}

    def load_all_features(self) -> List[Dict]:
        """Load all features"""
        results = []

        for feature in self.features:
            logging.info(f"\n{'='*80}")
            logging.info(f"Loading feature: {feature.upper()}")
            logging.info(f"{'='*80}")

            result = self.load_feature_incrementally(feature)
            results.append(result)

            if result["status"] in ["COMPLETED", "PARTIAL_SUCCESS"]:
                logging.info(
                    f"✓ {feature}: {result['files_processed']}/{result.get('total_files', 0)} files, "
                    f"{result['total_rows_loaded']:,} rows, {result['duration_seconds']:.2f}s"
                )
                if result.get("failed_files", 0) > 0:
                    logging.warning(f"  ⚠ {result['failed_files']} files failed")
            else:
                logging.error(f"✗ {feature}: {result.get('error', 'Unknown error')}")

        return results

    def optimize_all_tables(self):
        """Optimize all Delta tables"""
        logging.info("\n" + "=" * 80)
        logging.info("OPTIMIZING DELTA TABLES")
        logging.info("=" * 80)

        tables_path = self.bronze_base_path / "tables"
        if not tables_path.exists():
            logging.warning("No tables found to optimize")
            return

        for table_dir in tables_path.iterdir():
            if table_dir.is_dir() and DeltaTable.isDeltaTable(
                self.spark, str(table_dir)
            ):
                logging.info(f"Optimizing {table_dir.name}...")
                self._optimize_delta_table(table_dir)

                # Update last_optimized timestamp
                conn = self._get_postgres_connection()
                cur = conn.cursor()
                cur.execute(
                    f"""
                    UPDATE {self.postgres_config['schema']}.delta_table_stats
                    SET last_optimized = %s
                    WHERE table_name = %s
                """,
                    (datetime.now(), table_dir.name),
                )
                conn.commit()
                cur.close()

    def get_summary_report(self) -> Dict:
        """Get summary report of all operations"""
        try:
            conn = self._get_postgres_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)

            # Get overall stats
            cur.execute(
                f"""
                SELECT 
                    COUNT(*) as total_operations,
                    SUM(files_processed) as total_files,
                    SUM(rows_loaded) as total_rows,
                    AVG(total_duration_seconds) as avg_duration,
                    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as successful_operations,
                    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_operations
                FROM {self.postgres_config['schema']}.load_operations
            """
            )

            overall_stats = dict(cur.fetchone())

            # Get feature breakdown
            cur.execute(
                f"""
                SELECT 
                    feature_name,
                    COUNT(*) as operations,
                    SUM(files_processed) as files,
                    SUM(rows_loaded) as rows,
                    MAX(completed_at) as last_run
                FROM {self.postgres_config['schema']}.load_operations
                GROUP BY feature_name
                ORDER BY feature_name
            """
            )

            feature_stats = [dict(row) for row in cur.fetchall()]

            # Get table stats
            cur.execute(
                f"""
                SELECT 
                    table_name,
                    total_records,
                    total_files,
                    table_size_mb,
                    avg_quality_score,
                    last_updated
                FROM {self.postgres_config['schema']}.delta_table_stats
                ORDER BY total_records DESC
            """
            )

            table_stats = [dict(row) for row in cur.fetchall()]

            cur.close()

            return {
                "overall": overall_stats,
                "by_feature": feature_stats,
                "tables": table_stats,
            }

        except Exception as e:
            logging.error(f"Error generating summary report: {e}")
            return {}

    def print_summary_report(self):
        """Print formatted summary report"""
        report = self.get_summary_report()

        if not report:
            logging.warning("No report data available")
            return

        print("\n" + "=" * 80)
        print("BRONZE LAYER SUMMARY REPORT")
        print("=" * 80)

        # Overall stats
        overall = report.get("overall", {})
        print(f"\n📊 OVERALL STATISTICS:")
        print(f"  Total Operations: {overall.get('total_operations', 0)}")
        print(f"  Successful: {overall.get('successful_operations', 0)}")
        print(f"  Failed: {overall.get('failed_operations', 0)}")
        print(f"  Total Files Processed: {overall.get('total_files', 0):,}")
        print(f"  Total Rows Loaded: {overall.get('total_rows', 0):,}")
        print(f"  Average Duration: {overall.get('avg_duration', 0):.2f}s")

        # Feature breakdown
        print(f"\n📁 BY FEATURE:")
        for feature in report.get("by_feature", []):
            print(f"  {feature['feature_name'].upper()}:")
            print(f"    Operations: {feature['operations']}")
            print(f"    Files: {feature['files']:,}")
            print(f"    Rows: {feature['rows']:,}")
            print(f"    Last Run: {feature['last_run']}")

        # Table stats
        print(f"\n📋 DELTA TABLES:")
        for table in report.get("tables", []):
            print(f"  {table['table_name']}:")
            print(f"    Records: {table['total_records']:,}")
            print(f"    Files: {table['total_files']}")
            print(f"    Size: {table['table_size_mb']:.2f} MB")
            print(f"    Quality Score: {table['avg_quality_score']:.2f}")
            print(f"    Last Updated: {table['last_updated']}")

        print("\n" + "=" * 80)

    def verify_loaded_data(self):
        """Verify loaded data in Delta tables"""
        tables_path = self.bronze_base_path / "tables"

        if not tables_path.exists():
            logging.warning("No tables directory found")
            return []

        logging.info("\n" + "=" * 80)
        logging.info("VERIFYING DELTA TABLES")
        logging.info("=" * 80)

        tables = []
        for table_dir in sorted(tables_path.iterdir()):
            if table_dir.is_dir() and DeltaTable.isDeltaTable(
                self.spark, str(table_dir)
            ):
                try:
                    table_name = table_dir.name
                    tables.append(table_name)

                    # Get table details
                    details_df = self.spark.sql(
                        f"DESCRIBE DETAIL delta.`{str(table_dir)}`"
                    )
                    details = details_df.collect()[0]

                    # Get record count
                    count_df = self.spark.sql(
                        f"SELECT COUNT(*) as cnt FROM delta.`{str(table_dir)}`"
                    )
                    row_count = count_df.collect()[0]["cnt"]

                    # Get schema
                    schema_df = self.spark.sql(f"DESCRIBE delta.`{str(table_dir)}`")
                    columns = [
                        row["col_name"]
                        for row in schema_df.collect()
                        if row["col_name"] and not row["col_name"].startswith("#")
                    ]

                    print(f"\n📊 {table_name}")
                    print(f"  ├─ Records: {row_count:,}")
                    print(f"  ├─ Columns: {len(columns)}")
                    print(f"  ├─ Files: {details['numFiles']}")
                    print(f"  ├─ Size: {details['sizeInBytes'] / (1024**2):.2f} MB")
                    print(f"  └─ Partitions: {details['partitionColumns']}")

                    # Show sample data
                    if row_count > 0:
                        sample_df = self.spark.read.format("delta").load(str(table_dir))
                        print(f"\n  Sample columns (first 5):")
                        for col in columns[:5]:
                            print(f"    • {col}")

                except Exception as e:
                    logging.error(f"Error verifying table {table_dir.name}: {e}")

        print("\n" + "=" * 80)
        return tables

    def create_bronze_catalog(self):
        """Create Spark SQL database and catalog entries"""
        try:
            # Create database
            self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze_layer")

            tables_path = self.bronze_base_path / "tables"
            if not tables_path.exists():
                logging.warning("No tables to catalog")
                return

            logging.info("\n" + "=" * 80)
            logging.info("CREATING BRONZE CATALOG")
            logging.info("=" * 80)

            for table_dir in tables_path.iterdir():
                if table_dir.is_dir() and DeltaTable.isDeltaTable(
                    self.spark, str(table_dir)
                ):
                    table_name = table_dir.name

                    # Drop existing table if any
                    self.spark.sql(f"DROP TABLE IF EXISTS bronze_layer.{table_name}")

                    # Create external table
                    self.spark.sql(
                        f"""
                        CREATE TABLE bronze_layer.{table_name}
                        USING DELTA
                        LOCATION '{str(table_dir)}'
                    """
                    )

                    logging.info(f"✓ Cataloged: bronze_layer.{table_name}")

            # Show all tables
            tables_df = self.spark.sql("SHOW TABLES IN bronze_layer")
            print("\n📚 Available tables in bronze_layer:")
            for row in tables_df.collect():
                print(f"  • {row['tableName']}")

            print("\n" + "=" * 80)

        except Exception as e:
            logging.error(f"Error creating bronze catalog: {e}")

    def cleanup(self):
        """Cleanup resources"""
        try:
            if self.pg_conn and not self.pg_conn.closed:
                self.pg_conn.close()
                logging.info("PostgreSQL connection closed")

            if self.spark:
                self.spark.stop()
                logging.info("Spark session stopped")

        except Exception as e:
            logging.error(f"Error during cleanup: {e}")


def main():
    """Main execution function"""
    # Configure logging
    log_dir = Path("/opt/etl/logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "bronze_loader_spark.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logger = logging.getLogger(__name__)

    loader = None

    try:
        logger.info("=" * 80)
        logger.info("BRONZE LAYER LOADER - LOCAL STORAGE")
        logger.info("=" * 80)

        # Configuration for local storage
        bronze_base_path = "/opt/etl/bronze"
        source_data_path = "/opt/etl/output"

        postgres_config = {
            "host": "localhost",
            "port": "5432",
            "database": "excel_pipeline",
            "user": "postgres",
            "password": "password",
            "schema": "bronze_logs",
        }

        # Optional: Custom Spark configurations
        spark_configs = {
            "spark.sql.shuffle.partitions": "8",
            "spark.driver.memory": "4g",
            "spark.executor.memory": "4g",
        }

        # Initialize loader
        loader = BronzeLayerSparkLoader(
            bronze_base_path=bronze_base_path,
            source_data_path=source_data_path,
            postgres_config=postgres_config,
            spark_configs=spark_configs,
        )

        # Load all features
        logger.info("\n🚀 Starting incremental load...")
        results = loader.load_all_features()

        # Optimize tables
        logger.info("\n⚡ Optimizing Delta tables...")
        loader.optimize_all_tables()

        # Create catalog
        logger.info("\n📚 Creating Spark catalog...")
        loader.create_bronze_catalog()

        # Verify data
        logger.info("\n🔍 Verifying loaded data...")
        loader.verify_loaded_data()

        # Print summary report
        loader.print_summary_report()

        # Check for failures
        failed = [r for r in results if r["status"] == "FAILED"]
        partial = [r for r in results if r["status"] == "PARTIAL_SUCCESS"]

        if failed:
            logger.error(f"\n❌ {len(failed)} feature(s) failed completely")
            return False
        elif partial:
            logger.warning(f"\n⚠️  {len(partial)} feature(s) had partial failures")
            return True
        else:
            logger.info("\n✅ All features loaded successfully!")
            return True

    except Exception as e:
        logger.error(f"\n❌ Bronze layer load failed: {e}", exc_info=True)
        return False

    finally:
        if loader:
            loader.cleanup()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

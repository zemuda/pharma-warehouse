# bronze_loader_spark_local.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import *
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import hashlib
import logging
import sys
import psycopg2
from psycopg2.extras import RealDictCursor


class BronzeLayerSparkLoader:
    def __init__(
        self,
        bronze_base_path: str = "/opt/etl/bronze",  # Local path
        postgres_config: Dict = None,
        source_base_path: str = "/opt/etl/data/output",  # Local source path
    ):
        self.bronze_base_path = bronze_base_path
        self.source_base_path = source_base_path
        self.postgres_config = postgres_config or {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "database": os.getenv("DB_NAME", "pharma_warehouse"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "1234"),
            "schema": os.getenv("DB_BRONZE_LAYER_SCHEMA", "bronze_layer"),
        }

        self.features = ["sales", "purchases", "inventory", "master_data"]

        # Initialize Spark session with Delta Lake for local storage
        self.spark = self._create_spark_session()

        # Initialize PostgreSQL logging
        self._init_postgres_logging()

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake for local file system"""
        return (
            SparkSession.builder.appName("BronzeLayerSparkLoader")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.driver.memory", "2g")  # Adjust for your system
            .config("spark.executor.memory", "2g")
            .config(
                "spark.sql.warehouse.dir",
                f"file://{self.bronze_base_path}/spark-warehouse",
            )
            .config(
                "spark.delta.logStore.class",
                "org.apache.spark.sql.delta.storage.HDFSLogStore",
            )  # For local FS
            .getOrCreate()
        )

    def _get_postgres_connection(self):
        """Get direct PostgreSQL connection for DDL operations"""
        try:
            conn = psycopg2.connect(
                host=self.postgres_config["host"],
                port=self.postgres_config["port"],
                database=self.postgres_config["database"],
                user=self.postgres_config["user"],
                password=self.postgres_config["password"],
            )
            return conn
        except Exception as e:
            logging.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def _init_postgres_logging(self):
        """Initialize PostgreSQL logging tables using direct connection"""
        try:
            conn = self._get_postgres_connection()
            cur = conn.cursor()

            # Create schema and tables
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.postgres_config['schema']}")

            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.postgres_config['schema']}.load_operations (
                    operation_id SERIAL PRIMARY KEY,
                    feature_name VARCHAR(100),
                    load_path VARCHAR(500),
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    files_processed INTEGER,
                    rows_loaded BIGINT,
                    status VARCHAR(50),
                    error_message TEXT,
                    spark_application_id VARCHAR(100),
                    total_duration_seconds INTEGER
                )
            """
            )

            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.postgres_config['schema']}.schema_evolution (
                    evolution_id SERIAL PRIMARY KEY,
                    feature_name VARCHAR(100),
                    table_name VARCHAR(200),
                    change_type VARCHAR(50),
                    column_name VARCHAR(200),
                    old_data_type VARCHAR(100),
                    new_data_type VARCHAR(100),
                    change_date TIMESTAMP,
                    resolved BOOLEAN DEFAULT FALSE,
                    operation_id INTEGER REFERENCES {self.postgres_config['schema']}.load_operations(operation_id)
                )
            """
            )

            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.postgres_config['schema']}.file_processing (
                    file_id SERIAL PRIMARY KEY,
                    operation_id INTEGER REFERENCES {self.postgres_config['schema']}.load_operations(operation_id),
                    file_path VARCHAR(500),
                    file_size BIGINT,
                    records_count BIGINT,
                    processed_at TIMESTAMP,
                    status VARCHAR(50),
                    error_message TEXT,
                    data_quality_score DECIMAL(3,2)
                )
            """
            )

            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.postgres_config['schema']}.delta_table_stats (
                    table_name VARCHAR(200) PRIMARY KEY,
                    total_records BIGINT,
                    total_files INTEGER,
                    table_size_gb DECIMAL(10,2),
                    last_updated TIMESTAMP,
                    partition_columns TEXT[],
                    zorder_columns TEXT[],
                    avg_quality_score DECIMAL(3,2)
                )
            """
            )

            conn.commit()
            cur.close()
            conn.close()
            logging.info("PostgreSQL logging tables initialized successfully")

        except Exception as e:
            logging.error(f"Failed to initialize PostgreSQL logging: {e}")
            raise

    def _log_to_postgres(self, table: str, data: Dict):
        """Log data to PostgreSQL table using direct connection"""
        try:
            conn = self._get_postgres_connection()
            cur = conn.cursor()

            placeholders = ", ".join(["%s"] * len(data))
            columns = ", ".join(data.keys())
            values = tuple(data.values())

            query = f"INSERT INTO {self.postgres_config['schema']}.{table} ({columns}) VALUES ({placeholders})"
            cur.execute(query, values)

            conn.commit()
            cur.close()
            conn.close()

        except Exception as e:
            logging.error(f"Failed to log to PostgreSQL {table}: {e}")

    def discover_parquet_files(self, feature: str) -> List[Path]:
        """Discover all parquet files for a given feature using local file system"""
        feature_path = Path(self.source_base_path) / feature

        parquet_files = []
        if feature_path.exists():
            # Use local file system discovery
            for parquet_file in feature_path.rglob("*.parquet"):
                parquet_files.append(parquet_file)

        logging.info(f"Discovered {len(parquet_files)} parquet files for {feature}")
        return parquet_files

    def get_table_name_from_path(self, file_path: Path) -> str:
        """Convert file path to table name - group by feature and type"""
        try:
            # Convert to relative path from source base
            relative_path = file_path.relative_to(Path(self.source_base_path))
            path_parts = relative_path.parts

            # Extract feature and type (sales/cash_invoices/detailed)
            if len(path_parts) >= 3:
                table_parts = path_parts[:3]  # ['sales', 'cash_invoices', 'detailed']
            else:
                table_parts = path_parts

            table_name = "_".join(table_parts).lower()
            # Remove any file extensions and invalid characters
            table_name = table_name.replace(".parquet", "").replace("-", "_")
            return f"bronze_{table_name}"

        except Exception as e:
            logging.error(f"Error generating table name from {file_path}: {e}")
            return "bronze_unknown"

    def _enhance_with_metadata(self, df, file_path: Path, operation_id: int):
        """Add comprehensive metadata columns"""
        return (
            df.withColumn("source_file_path", lit(str(file_path)))
            .withColumn("source_file_name", lit(file_path.name))
            .withColumn("load_timestamp", current_timestamp())
            .withColumn("operation_batch_id", lit(operation_id))
            .withColumn(
                "data_hash", sha2(concat_ws("|", *[col(c) for c in df.columns]), 256)
            )
            .withColumn("ingestion_date", current_date())
            .withColumn(
                "spark_application_id", lit(self.spark.sparkContext.applicationId)
            )
        )

    def _apply_data_quality_checks(self, df, table_name: str):
        """Apply advanced data quality checks"""
        quality_df = df

        # Basic null checks for all columns
        for col_name in df.columns:
            quality_df = quality_df.withColumn(
                f"dq_{col_name}_null", when(col(col_name).isNull(), 1).otherwise(0)
            )

        # Calculate overall quality score
        null_columns = [f"dq_{col}_null" for col in df.columns]
        if null_columns:
            quality_df = quality_df.withColumn(
                "dq_quality_score",
                (
                    1 - (sum([col(c) for c in null_columns]) / lit(len(null_columns)))
                ).cast("decimal(3,2)"),
            )
        else:
            quality_df = quality_df.withColumn("dq_quality_score", lit(1.0))

        return quality_df

    def _deduplicate_data(self, df, primary_keys: List[str]):
        """Remove duplicate records using Spark window functions"""
        if not primary_keys:
            return df

        window_spec = Window.partitionBy(primary_keys).orderBy(
            col("load_timestamp").desc(), col("data_hash")
        )

        return (
            df.withColumn("_row_num", row_number().over(window_spec))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )

    def _get_primary_keys(self, table_name: str) -> List[str]:
        """Define primary keys based on table type"""
        if "detailed" in table_name:
            return ["invoice_id", "source_file_path"]
        elif "summarized" in table_name:
            return ["period_id", "source_file_path"]
        else:
            return ["source_file_path"]  # Fallback

    def _write_to_delta_table(self, df, table_name: str, operation_id: int):
        """Write DataFrame to Delta table on local file system"""
        try:
            table_full_path = f"file://{self.bronze_base_path}/tables/{table_name}"

            # Ensure directory exists
            os.makedirs(f"{self.bronze_base_path}/tables/{table_name}", exist_ok=True)

            if DeltaTable.isDeltaTable(self.spark, table_full_path):
                # Merge into existing table
                delta_table = DeltaTable.forPath(self.spark, table_full_path)
                primary_keys = self._get_primary_keys(table_name)

                if primary_keys and all(key in df.columns for key in primary_keys):
                    merge_condition = " AND ".join(
                        [f"target.{pk} = source.{pk}" for pk in primary_keys]
                    )

                    (
                        delta_table.alias("target")
                        .merge(df.alias("source"), merge_condition)
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                    logging.info(f"Merged data into Delta table: {table_name}")
                else:
                    # Append if no primary keys or keys missing
                    df.write.format("delta").mode("append").save(table_full_path)
                    logging.info(f"Appended data to Delta table: {table_name}")

            else:
                # Create new Delta table with partitioning
                partition_cols = ["ingestion_date"]
                if any(year_col in df.columns for year_col in ["year", "invoice_year"]):
                    partition_cols.append("year")
                if any(
                    month_col in df.columns for month_col in ["month", "invoice_month"]
                ):
                    partition_cols.append("month")

                # Only partition by columns that exist
                existing_partition_cols = [
                    col for col in partition_cols if col in df.columns
                ]

                (
                    df.write.format("delta")
                    .mode("overwrite")
                    .option("delta.autoOptimize.optimizeWrite", "true")
                    .partitionBy(
                        existing_partition_cols if existing_partition_cols else []
                    )
                    .save(table_full_path)
                )
                logging.info(f"Created new Delta table: {table_name}")

            # Optimize table
            self._optimize_delta_table(table_full_path)

            # Log table statistics
            self._log_delta_table_stats(table_name, table_full_path, operation_id)

            return True

        except Exception as e:
            logging.error(f"Error writing to Delta table {table_name}: {e}")
            return False

    def _optimize_delta_table(self, table_path: str):
        """Optimize Delta table performance"""
        try:
            # Z-Order optimization on common query columns
            self.spark.sql(
                f"""
                OPTIMIZE delta.`{table_path}`
                ZORDER BY (load_timestamp, ingestion_date)
            """
            )

            # Vacuum old files (keep 7 days)
            self.spark.sql(f"VACUUM delta.`{table_path}` RETAIN 168 HOURS")

            logging.info(f"Optimized Delta table: {table_path}")

        except Exception as e:
            logging.warning(f"Optimization failed for {table_path}: {e}")

    def _log_delta_table_stats(
        self, table_name: str, table_path: str, operation_id: int
    ):
        """Log Delta table statistics to PostgreSQL"""
        try:
            # Get table details
            details_df = self.spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`")
            details = details_df.collect()[0] if details_df.count() > 0 else {}

            # Get data quality metrics
            quality_stats_df = self.spark.sql(
                f"""
                SELECT 
                    COUNT(*) as total_records,
                    AVG(dq_quality_score) as avg_quality_score
                FROM delta.`{table_path}`
            """
            )
            quality_stats = (
                quality_stats_df.collect()[0]
                if quality_stats_df.count() > 0
                else {"total_records": 0, "avg_quality_score": 0.0}
            )

            stats_data = {
                "table_name": table_name,
                "total_records": quality_stats["total_records"],
                "total_files": details.get("numFiles", 0),
                "table_size_gb": round(details.get("sizeInBytes", 0) / (1024**3), 2),
                "last_updated": datetime.now(),
                "partition_columns": str(details.get("partitionColumns", [])),
                "zorder_columns": "['load_timestamp', 'ingestion_date']",
                "avg_quality_score": (
                    float(quality_stats["avg_quality_score"])
                    if quality_stats["avg_quality_score"]
                    else 0.0
                ),
            }

            # Update or insert stats
            conn = self._get_postgres_connection()
            cur = conn.cursor()

            placeholders = ", ".join(["%s"] * len(stats_data))
            columns = ", ".join(stats_data.keys())
            update_clause = ", ".join(
                [f"{k} = EXCLUDED.{k}" for k in stats_data.keys()]
            )

            query = f"""
                INSERT INTO {self.postgres_config['schema']}.delta_table_stats ({columns}) 
                VALUES ({placeholders})
                ON CONFLICT (table_name) 
                DO UPDATE SET {update_clause}
            """

            cur.execute(query, tuple(stats_data.values()))
            conn.commit()
            cur.close()
            conn.close()

        except Exception as e:
            logging.error(f"Error logging Delta table stats: {e}")

    def load_file_incrementally(
        self, file_path: Path, operation_id: int
    ) -> Tuple[bool, int]:
        """Load a single parquet file using Spark"""
        try:
            table_name = self.get_table_name_from_path(file_path)
            feature = file_path.relative_to(Path(self.source_base_path)).parts[0]

            logging.info(f"Processing: {file_path} -> {table_name}")

            # Read parquet file using file:// protocol for local files
            file_uri = f"file://{file_path}"
            df = self.spark.read.parquet(file_uri)

            # Enhance with metadata
            enhanced_df = self._enhance_with_metadata(df, file_path, operation_id)

            # Apply data quality checks
            quality_df = self._apply_data_quality_checks(enhanced_df, table_name)

            # Deduplicate data
            primary_keys = self._get_primary_keys(table_name)
            final_df = self._deduplicate_data(quality_df, primary_keys)

            # Write to Delta table
            success = self._write_to_delta_table(final_df, table_name, operation_id)

            if success:
                row_count = final_df.count()

                # Log file processing
                file_stats = {
                    "operation_id": operation_id,
                    "file_path": str(file_path),
                    "file_size": file_path.stat().st_size,
                    "records_count": row_count,
                    "processed_at": datetime.now(),
                    "status": "SUCCESS",
                    "data_quality_score": 0.9,  # Default, can be calculated from actual data
                }
                self._log_to_postgres("file_processing", file_stats)

                logging.info(f"Successfully processed {file_path} - {row_count} rows")
                return True, row_count
            else:
                raise Exception("Failed to write to Delta table")

        except Exception as e:
            # Log failure
            file_stats = {
                "operation_id": operation_id,
                "file_path": str(file_path),
                "file_size": file_path.stat().st_size if file_path.exists() else 0,
                "processed_at": datetime.now(),
                "status": "FAILED",
                "error_message": str(e),
            }
            self._log_to_postgres("file_processing", file_stats)

            logging.error(f"Error processing {file_path}: {e}")
            return False, 0

    def load_feature_incrementally(self, feature: str) -> Dict:
        """Load all files for a specific feature"""
        operation_data = {
            "feature_name": feature,
            "load_path": f"{self.source_base_path}/{feature}",
            "started_at": datetime.now(),
            "status": "STARTED",
            "spark_application_id": self.spark.sparkContext.applicationId,
        }

        # Log operation start
        self._log_to_postgres("load_operations", operation_data)
        operation_id = self._get_last_operation_id()

        try:
            # Discover files
            all_files = self.discover_parquet_files(feature)

            if not all_files:
                logging.warning(f"No files found for feature: {feature}")
                operation_data.update(
                    {
                        "status": "COMPLETED",
                        "files_processed": 0,
                        "rows_loaded": 0,
                        "completed_at": datetime.now(),
                        "total_duration_seconds": 0,
                    }
                )
                self._log_to_postgres("load_operations", operation_data)
                return {"feature": feature, "status": "NO_FILES"}

            files_processed = 0
            total_rows_loaded = 0

            # Process each file
            for file_path in all_files:
                success, rows_loaded = self.load_file_incrementally(
                    file_path, operation_id
                )
                if success:
                    files_processed += 1
                    total_rows_loaded += rows_loaded

            # Update operation log
            duration = (datetime.now() - operation_data["started_at"]).total_seconds()
            operation_data.update(
                {
                    "status": "COMPLETED",
                    "files_processed": files_processed,
                    "rows_loaded": total_rows_loaded,
                    "completed_at": datetime.now(),
                    "total_duration_seconds": int(duration),
                }
            )
            self._log_to_postgres("load_operations", operation_data)

            return {
                "feature": feature,
                "files_processed": files_processed,
                "total_rows_loaded": total_rows_loaded,
                "status": "SUCCESS",
                "duration_seconds": int(duration),
            }

        except Exception as e:
            # Log operation failure
            operation_data.update(
                {
                    "status": "FAILED",
                    "error_message": str(e),
                    "completed_at": datetime.now(),
                }
            )
            self._log_to_postgres("load_operations", operation_data)

            return {"feature": feature, "error": str(e), "status": "FAILED"}

    def _get_last_operation_id(self) -> int:
        """Get the last operation ID from PostgreSQL"""
        try:
            conn = self._get_postgres_connection()
            cur = conn.cursor()

            cur.execute(
                f"SELECT MAX(operation_id) FROM {self.postgres_config['schema']}.load_operations"
            )
            result = cur.fetchone()

            cur.close()
            conn.close()

            return result[0] if result[0] else 1
        except Exception as e:
            logging.error(f"Error getting last operation ID: {e}")
            return 1

    def load_all_features(self):
        """Load all features incrementally"""
        results = []
        for feature in self.features:
            logging.info(f"\n{'='*60}")
            logging.info(f"Loading feature: {feature}")
            logging.info(f"{'='*60}")

            result = self.load_feature_incrementally(feature)
            results.append(result)

            if result["status"] == "SUCCESS":
                logging.info(
                    f"Completed {feature}: {result['files_processed']} files, {result['total_rows_loaded']} rows, {result['duration_seconds']}s"
                )
            else:
                logging.error(
                    f"Failed {feature}: {result.get('error', 'Unknown error')}"
                )

        return results

    def create_bronze_catalog(self):
        """Create Spark catalog entries for bronze tables"""
        try:
            self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze_layer")

            bronze_tables_path = f"{self.bronze_base_path}/tables"
            table_dirs = [
                f for f in Path(bronze_tables_path).glob("bronze_*") if f.is_dir()
            ]

            for table_dir in table_dirs:
                table_name = table_dir.name
                table_full_path = f"file://{table_dir}"

                if DeltaTable.isDeltaTable(self.spark, table_full_path):
                    self.spark.sql(
                        f"""
                        CREATE TABLE IF NOT EXISTS bronze_layer.{table_name}
                        USING DELTA
                        LOCATION '{table_full_path}'
                    """
                    )
                    logging.info(f"Created catalog entry for: {table_name}")

        except Exception as e:
            logging.error(f"Error creating bronze catalog: {e}")

    def verify_loaded_data(self):
        """Verify that data is loaded into Delta tables"""
        try:
            bronze_tables_path = f"{self.bronze_base_path}/tables"
            table_dirs = [
                f for f in Path(bronze_tables_path).glob("bronze_*") if f.is_dir()
            ]

            logging.info(f"\n{'='*60}")
            logging.info("BRONZE TABLES IN DELTA LAKE")
            logging.info(f"{'='*60}")

            for table_dir in table_dirs:
                table_name = table_dir.name
                table_path = f"file://{table_dir}"

                try:
                    if DeltaTable.isDeltaTable(self.spark, table_path):
                        details_df = self.spark.sql(
                            f"DESCRIBE DETAIL delta.`{table_path}`"
                        )
                        details = details_df.collect()[0]

                        row_count_df = self.spark.sql(
                            f"SELECT COUNT(*) as cnt FROM delta.`{table_path}`"
                        )
                        row_count = row_count_df.collect()[0]["cnt"]

                        columns_df = self.spark.sql(f"DESCRIBE delta.`{table_path}`")
                        columns = [
                            row["col_name"]
                            for row in columns_df.collect()
                            if row["col_name"] not in ["", "Parting"]
                        ]

                        logging.info(f"\nTable: {table_name}")
                        logging.info(f"  Rows: {row_count:,}")
                        logging.info(f"  Columns: {len(columns)}")
                        logging.info(
                            f"  Size: {details['sizeInBytes'] / (1024**3):.2f} GB"
                        )
                        logging.info(f"  Files: {details['numFiles']}")
                        logging.info(f"  Partitions: {details['partitionColumns']}")

                        # Show sample of data
                        sample_df = self.spark.sql(
                            f"SELECT * FROM delta.`{table_path}` LIMIT 5"
                        )
                        logging.info(f"  Sample data: {sample_df.count()} rows")

                    else:
                        logging.warning(f"Table {table_name} is not a Delta table")

                except Exception as e:
                    logging.error(f"Error reading table {table_name}: {e}")

            return [td.name for td in table_dirs]

        except Exception as e:
            logging.error(f"Error verifying loaded data: {e}")
            return []


def run_bronze_local():
    """Run bronze layer with local storage"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("/opt/etl/logs/bronze_loader.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logger = logging.getLogger(__name__)

    try:
        logger.info("Starting Bronze Layer with Local Storage")

        # Configuration for local setup
        postgres_config = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "database": os.getenv("DB_NAME", "pharma_warehouse"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "1234"),
            "schema": os.getenv("DB_BRONZE_LAYER_SCHEMA", "bronze_layer"),
        }

        loader = BronzeLayerSparkLoader(
            bronze_base_path="/opt/etl/bronze",
            postgres_config=postgres_config,
            source_base_path="/opt/etl/data/output",
        )

        # Load all features
        results = loader.load_all_features()

        # Create catalog entries
        loader.create_bronze_catalog()

        # Verify loaded data
        loader.verify_loaded_data()

        # Log results
        for result in results:
            if result["status"] == "SUCCESS":
                logger.info(
                    f"✅ Feature {result['feature']}: {result['files_processed']} files, {result['total_rows_loaded']} rows, {result['duration_seconds']}s"
                )
            else:
                logger.error(
                    f"❌ Feature {result['feature']} failed: {result.get('error', 'Unknown error')}"
                )

        logger.info("🎉 Bronze Layer processing completed successfully!")
        return True

    except Exception as e:
        logger.error(f"💥 Bronze Layer processing failed: {e}")
        return False
    finally:
        if "loader" in locals():
            loader.spark.stop()


if __name__ == "__main__":
    success = run_bronze_local()
    sys.exit(0 if success else 1)

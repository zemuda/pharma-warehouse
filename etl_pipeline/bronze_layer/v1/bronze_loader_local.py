# bronze_loader_production.py

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
from psycopg2.extras import RealDictCursor, execute_batch
import hashlib
import json


class ProductionBronzeLoader:
    def __init__(
        self,
        bronze_base_path: str = "/opt/etl/bronze",
        source_data_path: str = "/opt/etl/output",
        postgres_config: Dict = None,
        spark_configs: Dict = None,
        enable_scd2: bool = True,
        enable_quality_columns: bool = True,
        enable_zorder: bool = True,
    ):
        """
        Production Bronze Layer Loader with all advanced features

        Args:
            bronze_base_path: Local path for bronze Delta tables
            source_data_path: Local path where parquet files are stored
            postgres_config: PostgreSQL connection config
            spark_configs: Additional Spark configurations
            enable_scd2: Enable SCD Type 2 historical tracking
            enable_quality_columns: Add per-column quality flags
            enable_zorder: Enable Z-ORDER optimization
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

        # Feature flags
        self.enable_scd2 = enable_scd2
        self.enable_quality_columns = enable_quality_columns
        self.enable_zorder = enable_zorder

        # Initialize Spark session
        self.spark = self._create_spark_session()

        # Initialize PostgreSQL logging
        self._init_postgres_logging()

        # PostgreSQL connection pool
        self.pg_conn = None

    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session for local file storage"""
        default_configs = {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # Local storage optimizations
            "spark.sql.shuffle.partitions": "8",
            "spark.default.parallelism": "4",
            "spark.sql.files.maxPartitionBytes": "128MB",
            # Adaptive query execution
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
            # Memory management
            "spark.driver.memory": "4g",
            "spark.executor.memory": "4g",
            "spark.memory.fraction": "0.8",
            "spark.memory.storageFraction": "0.3",
            # Delta Lake configs
            "spark.databricks.delta.retentionDurationCheck.enabled": "false",
            "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
            "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "true",
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
            # Compression
            "spark.sql.parquet.compression.codec": "snappy",
            # Serialization
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.kryo.registrationRequired": "false",
            # Local file system
            "spark.hadoop.fs.defaultFS": "file:///",
        }

        default_configs.update(self.spark_configs)

        builder = SparkSession.builder.appName("ProductionBronzeLoader")

        for key, value in default_configs.items():
            builder = builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        logging.info(f"✓ Spark UI: {spark.sparkContext.uiWebUrl}")

        return spark

    def _get_postgres_connection(self):
        """Get PostgreSQL connection"""
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
        """Initialize PostgreSQL logging tables with all advanced features"""
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
                    files_skipped INTEGER DEFAULT 0,
                    rows_loaded BIGINT DEFAULT 0,
                    status VARCHAR(50) NOT NULL,
                    error_message TEXT,
                    spark_application_id VARCHAR(100),
                    total_duration_seconds INTEGER,
                    scd2_enabled BOOLEAN DEFAULT FALSE,
                    quality_checks_enabled BOOLEAN DEFAULT FALSE
                )
            """
            )

            # Schema evolution table (ACTIVE TRACKING)
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
                    operation_id INTEGER REFERENCES {self.postgres_config['schema']}.load_operations(operation_id),
                    schema_before JSONB,
                    schema_after JSONB
                )
            """
            )

            # File processing table with tracking
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.postgres_config['schema']}.file_processing (
                    file_id SERIAL PRIMARY KEY,
                    operation_id INTEGER NOT NULL REFERENCES {self.postgres_config['schema']}.load_operations(operation_id),
                    file_path VARCHAR(500) NOT NULL,
                    file_hash VARCHAR(64) NOT NULL,
                    file_size BIGINT,
                    file_modified_time TIMESTAMP,
                    records_count BIGINT,
                    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    status VARCHAR(50) NOT NULL,
                    error_message TEXT,
                    data_quality_score DECIMAL(5,2),
                    processing_time_seconds DECIMAL(10,2),
                    UNIQUE(file_path, file_hash)
                )
            """
            )

            # Delta table stats with Z-ORDER tracking
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.postgres_config['schema']}.delta_table_stats (
                    table_name VARCHAR(200) PRIMARY KEY,
                    total_records BIGINT,
                    current_records BIGINT,
                    historical_records BIGINT,
                    total_files INTEGER,
                    table_size_mb DECIMAL(10,2),
                    last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
                    partition_columns TEXT[],
                    zorder_columns TEXT[],
                    avg_quality_score DECIMAL(5,2),
                    last_optimized TIMESTAMP,
                    last_zorder_optimized TIMESTAMP,
                    scd2_enabled BOOLEAN DEFAULT FALSE
                )
            """
            )

            # Z-ORDER optimization log
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.postgres_config['schema']}.zorder_optimization_log (
                    optimization_id SERIAL PRIMARY KEY,
                    table_name VARCHAR(200) NOT NULL,
                    zorder_columns TEXT[],
                    optimization_started TIMESTAMP NOT NULL DEFAULT NOW(),
                    optimization_completed TIMESTAMP,
                    files_before INTEGER,
                    files_after INTEGER,
                    size_before_mb DECIMAL(10,2),
                    size_after_mb DECIMAL(10,2),
                    duration_seconds INTEGER,
                    status VARCHAR(50)
                )
            """
            )

            # Batch distribution analytics
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.postgres_config['schema']}.batch_distribution (
                    batch_id SERIAL PRIMARY KEY,
                    table_name VARCHAR(200) NOT NULL,
                    operation_id INTEGER REFERENCES {self.postgres_config['schema']}.load_operations(operation_id),
                    batch_date DATE NOT NULL,
                    records_loaded BIGINT,
                    records_updated BIGINT,
                    records_inserted BIGINT,
                    avg_quality_score DECIMAL(5,2),
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """
            )

            # Create indexes
            indexes = [
                f"CREATE INDEX IF NOT EXISTS idx_load_ops_feature ON {self.postgres_config['schema']}.load_operations(feature_name, started_at DESC)",
                f"CREATE INDEX IF NOT EXISTS idx_load_ops_status ON {self.postgres_config['schema']}.load_operations(status)",
                f"CREATE INDEX IF NOT EXISTS idx_file_proc_operation ON {self.postgres_config['schema']}.file_processing(operation_id)",
                f"CREATE INDEX IF NOT EXISTS idx_file_proc_hash ON {self.postgres_config['schema']}.file_processing(file_hash)",
                f"CREATE INDEX IF NOT EXISTS idx_schema_evo_table ON {self.postgres_config['schema']}.schema_evolution(table_name, change_date DESC)",
                f"CREATE INDEX IF NOT EXISTS idx_batch_dist_table ON {self.postgres_config['schema']}.batch_distribution(table_name, batch_date DESC)",
            ]

            for idx_sql in indexes:
                try:
                    cur.execute(idx_sql)
                except psycopg2.Error:
                    pass

            conn.commit()
            logging.info("✓ PostgreSQL logging tables initialized")

        except Exception as e:
            logging.error(f"Failed to initialize PostgreSQL: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if cur:
                cur.close()

    def _log_to_postgres(self, table: str, data: Dict) -> Optional[int]:
        """Log data to PostgreSQL table and return ID"""
        try:
            conn = self._get_postgres_connection()
            cur = conn.cursor()

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

            set_clause = ", ".join([f"{col} = %s" for col in data.keys()])
            values = list(data.values()) + [record_id]

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

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of file for change detection"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _is_file_already_processed(self, file_path: Path, file_hash: str) -> bool:
        """Check if file was already processed (TRUE INCREMENTAL)"""
        try:
            conn = self._get_postgres_connection()
            cur = conn.cursor()

            cur.execute(
                f"""
                SELECT COUNT(*) FROM {self.postgres_config['schema']}.file_processing
                WHERE file_path = %s AND file_hash = %s AND status = 'SUCCESS'
            """,
                (str(file_path), file_hash),
            )

            count = cur.fetchone()[0]
            cur.close()

            return count > 0

        except Exception as e:
            logging.error(f"Error checking file processing status: {e}")
            return False

    def discover_parquet_files(self, feature: str) -> List[Path]:
        """Discover parquet files"""
        feature_path = self.source_data_path / feature

        if not feature_path.exists():
            logging.warning(f"Feature path does not exist: {feature_path}")
            return []

        try:
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
        try:
            rel_path = file_path.relative_to(self.source_data_path)
            parts = rel_path.parts[:-1]
            relevant_parts = [p for p in parts if not p.isdigit()]
            table_name = "_".join(relevant_parts).lower()
            return f"bronze_{table_name}"
        except ValueError:
            return f"bronze_{file_path.stem}"

    def _get_table_schema(self, table_path: Path) -> Dict[str, str]:
        """Get current schema of Delta table"""
        try:
            if not DeltaTable.isDeltaTable(self.spark, str(table_path)):
                return {}

            df = self.spark.read.format("delta").load(str(table_path))
            schema = {}
            for field in df.schema.fields:
                if not field.name.startswith("_"):  # Skip metadata columns
                    schema[field.name] = field.dataType.simpleString()
            return schema
        except Exception as e:
            logging.error(f"Error getting table schema: {e}")
            return {}

    def _detect_schema_changes(
        self,
        old_schema: Dict[str, str],
        new_schema: Dict[str, str],
        table_name: str,
        operation_id: int,
    ) -> List[Dict]:
        """Detect and log schema changes (SCHEMA EVOLUTION)"""
        changes = []

        # Detect new columns
        for col, dtype in new_schema.items():
            if col not in old_schema:
                changes.append(
                    {
                        "change_type": "COLUMN_ADDED",
                        "column_name": col,
                        "new_data_type": dtype,
                        "old_data_type": None,
                    }
                )

        # Detect removed columns
        for col, dtype in old_schema.items():
            if col not in new_schema:
                changes.append(
                    {
                        "change_type": "COLUMN_REMOVED",
                        "column_name": col,
                        "old_data_type": dtype,
                        "new_data_type": None,
                    }
                )

        # Detect type changes
        for col in set(old_schema.keys()) & set(new_schema.keys()):
            if old_schema[col] != new_schema[col]:
                changes.append(
                    {
                        "change_type": "TYPE_CHANGED",
                        "column_name": col,
                        "old_data_type": old_schema[col],
                        "new_data_type": new_schema[col],
                    }
                )

        # Log changes to PostgreSQL
        if changes:
            logging.warning(f"⚠️  Schema changes detected in {table_name}:")
            for change in changes:
                logging.warning(f"  - {change['change_type']}: {change['column_name']}")

                # Log to database
                log_data = {
                    "feature_name": table_name.replace("bronze_", ""),
                    "table_name": table_name,
                    "change_type": change["change_type"],
                    "column_name": change["column_name"],
                    "old_data_type": change["old_data_type"],
                    "new_data_type": change["new_data_type"],
                    "operation_id": operation_id,
                    "schema_before": json.dumps(old_schema),
                    "schema_after": json.dumps(new_schema),
                }
                self._log_to_postgres("schema_evolution", log_data)

        return changes

    def _enhance_with_metadata(self, df, file_path: Path, operation_id: int):
        """Add metadata columns including data hash for SCD2"""
        enhanced_df = (
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
        )

        # Add data hash for change detection (SCD2)
        if self.enable_scd2:
            # Create hash from business columns (exclude metadata)
            business_cols = [c for c in df.columns if not c.startswith("_")]
            enhanced_df = enhanced_df.withColumn(
                "_data_hash",
                sha2(
                    concat_ws(
                        "|",
                        *[
                            coalesce(col(c).cast("string"), lit("NULL"))
                            for c in business_cols
                        ],
                    ),
                    256,
                ),
            )

        return enhanced_df

    def _apply_data_quality_checks(self, df, table_name: str):
        """Apply comprehensive data quality checks"""
        quality_df = df

        if self.enable_quality_columns:
            # Add per-column null checks
            business_cols = [c for c in df.columns if not c.startswith("_")]

            for col_name in business_cols:
                quality_df = quality_df.withColumn(
                    f"_dq_{col_name}_null", when(col(col_name).isNull(), 1).otherwise(0)
                )

            # Add overall quality score
            dq_columns = [f"_dq_{c}_null" for c in business_cols]
            if dq_columns:
                quality_df = quality_df.withColumn(
                    "_dq_quality_score",
                    (
                        1 - (sum([col(c) for c in dq_columns]) / lit(len(dq_columns)))
                    ).cast("decimal(5,2)"),
                )

            # Add data freshness if date columns exist
            date_columns = [
                f.name
                for f in df.schema.fields
                if isinstance(f.dataType, (DateType, TimestampType))
                and not f.name.startswith("_")
            ]

            if date_columns:
                quality_df = quality_df.withColumn(
                    "_dq_data_freshness_days",
                    datediff(current_date(), col(date_columns[0])),
                )
        else:
            # Just add simple quality score
            business_cols = [c for c in df.columns if not c.startswith("_")]
            null_counts = [when(col(c).isNull(), 1).otherwise(0) for c in business_cols]

            if null_counts:
                quality_df = quality_df.withColumn(
                    "_dq_quality_score",
                    (1 - (sum(null_counts) / lit(len(null_counts)))).cast(
                        "decimal(5,2)"
                    ),
                )

        return quality_df

    def _apply_scd2_logic(
        self, df, table_path: Path, primary_keys: List[str]
    ) -> Tuple[bool, Dict]:
        """Apply SCD Type 2 (Slowly Changing Dimension) logic"""

        if not self.enable_scd2:
            # Just do simple merge without history
            return False, {}

        table_path_str = str(table_path)

        if not DeltaTable.isDeltaTable(self.spark, table_path_str):
            # First load - add SCD2 columns
            scd2_df = (
                df.withColumn("_valid_from", current_timestamp())
                .withColumn("_valid_to", lit(None).cast(TimestampType()))
                .withColumn("_is_current", lit(True))
                .withColumn("_record_version", lit(1))
            )
            return True, {"initial_load": True, "df": scd2_df}

        # Existing table - apply SCD2 merge logic
        delta_table = DeltaTable.forPath(self.spark, table_path_str)

        # Prepare source with SCD2 columns
        source_df = (
            df.withColumn("_valid_from", current_timestamp())
            .withColumn("_valid_to", lit(None).cast(TimestampType()))
            .withColumn("_is_current", lit(True))
            .withColumn("_record_version", lit(1))
        )

        if not primary_keys:
            return False, {}

        # Build merge condition on primary keys
        merge_condition = " AND ".join(
            [f"target.{pk} = source.{pk}" for pk in primary_keys]
        )
        merge_condition += " AND target._is_current = true"

        # SCD2 Merge Logic:
        # 1. If record exists and data_hash is SAME: no action
        # 2. If record exists and data_hash is DIFFERENT:
        #    - Close old record (set _valid_to, _is_current=false)
        #    - Insert new record with new version
        # 3. If record doesn't exist: insert new

        (
            delta_table.alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdate(
                condition="target._data_hash != source._data_hash",
                set={"_valid_to": "source._valid_from", "_is_current": "false"},
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Insert updated versions of changed records
        changed_records = (
            source_df.alias("source")
            .join(
                delta_table.toDF().alias("target"),
                [col(f"source.{pk}") == col(f"target.{pk}") for pk in primary_keys],
                "inner",
            )
            .where(
                "target._is_current = false AND target._data_hash != source._data_hash"
            )
            .select(
                *[
                    col(f"source.{c}").alias(c)
                    for c in source_df.columns
                    if c != "_record_version"
                ],
                (col("target._record_version") + 1).alias("_record_version"),
            )
        )

        if changed_records.count() > 0:
            changed_records.write.format("delta").mode("append").save(table_path_str)

        stats = {"scd2_applied": True, "records_updated": changed_records.count()}

        return True, stats

    def _get_primary_keys(self, table_name: str, df) -> List[str]:
        """Infer primary keys from table and data"""
        pks = []

        # Check for common ID columns
        id_columns = [
            c for c in df.columns if c.lower().endswith("_id") or c.lower() == "id"
        ]
        if id_columns:
            pks.extend(id_columns[:2])

        # Add date column if exists
        date_columns = [
            c for c in df.columns if "date" in c.lower() and not c.startswith("_")
        ]
        if date_columns and date_columns[0] not in pks:
            pks.append(date_columns[0])

        # Fallback
        if not pks:
            pks = ["_source_file_path"]

        return pks

    def _write_to_delta_table(
        self, df, table_name: str, operation_id: int
    ) -> Tuple[bool, Dict]:
        """Write DataFrame to Delta table with SCD2 support"""
        try:
            table_path = self.bronze_base_path / "tables" / table_name
            table_path.mkdir(parents=True, exist_ok=True)
            table_path_str = str(table_path)

            # Get old schema for change detection
            old_schema = self._get_table_schema(table_path)

            # Get new schema (business columns only)
            new_schema = {}
            for field in df.schema.fields:
                if not field.name.startswith("_"):
                    new_schema[field.name] = field.dataType.simpleString()

            # Detect schema changes
            if old_schema:
                self._detect_schema_changes(
                    old_schema, new_schema, table_name, operation_id
                )

            stats = {}

            # Apply SCD2 logic if enabled
            if self.enable_scd2:
                primary_keys = self._get_primary_keys(table_name, df)
                scd2_applied, scd2_stats = self._apply_scd2_logic(
                    df, table_path, primary_keys
                )
                stats.update(scd2_stats)

                if scd2_stats.get("initial_load"):
                    df = scd2_stats["df"]
                    partition_cols = ["_ingestion_date"]

                    (
                        df.write.format("delta")
                        .mode("overwrite")
                        .partitionBy(partition_cols)
                        .option("overwriteSchema", "true")
                        .save(table_path_str)
                    )
                    logging.info(f"✓ Created SCD2 table: {table_name}")
            else:
                # Simple merge without history
                if DeltaTable.isDeltaTable(self.spark, table_path_str):
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
                    else:
                        df.write.format("delta").mode("append").save(table_path_str)
                else:
                    partition_cols = ["_ingestion_date"]
                    (
                        df.write.format("delta")
                        .mode("overwrite")
                        .partitionBy(partition_cols)
                        .option("overwriteSchema", "true")
                        .save(table_path_str)
                    )

            stats["write_success"] = True
            return True, stats

        except Exception as e:
            logging.error(f"Error writing to Delta table {table_name}: {e}")
            return False, {"error": str(e)}

    def _get_zorder_columns(self, table_name: str, df) -> List[str]:
        """Determine optimal Z-ORDER columns"""
        zorder_cols = []

        # Always include load timestamp
        zorder_cols.append("_load_timestamp")

        # Add ingestion date
        zorder_cols.append("_ingestion_date")

        # Add ID columns (good for point lookups)
        id_cols = [
            c for c in df.columns if c.lower().endswith("_id") or c.lower() == "id"
        ]
        if id_cols:
            zorder_cols.extend(id_cols[:2])  # Max 2 ID columns

        # Add date columns (good for range queries)
        date_cols = [
            c
            for c in df.columns
            if "date" in c.lower() and not c.startswith("_") and c not in zorder_cols
        ]
        if date_cols:
            zorder_cols.append(date_cols[0])

        # Add SCD2 columns if enabled
        if self.enable_scd2 and "_is_current" in df.columns:
            zorder_cols.append("_is_current")

        # Limit to 4-5 columns for optimal performance
        return zorder_cols[:5]

    def _optimize_delta_table(self, table_path: Path, table_name: str) -> Dict:
        """Optimize Delta table with Z-ORDER"""
        optimization_start = datetime.now()
        stats = {}

        try:
            table_path_str = str(table_path)

            # Get stats before optimization
            details_before = self.spark.sql(
                f"DESCRIBE DETAIL delta.`{table_path_str}`"
            ).collect()[0]
            files_before = details_before["numFiles"]
            size_before_mb = round(details_before["sizeInBytes"] / (1024**2), 2)

            # Run basic OPTIMIZE
            self.spark.sql(f"OPTIMIZE delta.`{table_path_str}`")
            logging.info(f"  ✓ Basic OPTIMIZE completed for {table_name}")

            # Run Z-ORDER if enabled
            if self.enable_zorder:
                # Get sample to determine Z-ORDER columns
                sample_df = (
                    self.spark.read.format("delta").load(table_path_str).limit(1)
                )
                zorder_cols = self._get_zorder_columns(table_name, sample_df)

                if zorder_cols:
                    zorder_cols_str = ", ".join(zorder_cols)
                    self.spark.sql(
                        f"""
                        OPTIMIZE delta.`{table_path_str}`
                        ZORDER BY ({zorder_cols_str})
                    """
                    )
                    logging.info(f"  ✓ Z-ORDER applied on: {zorder_cols_str}")
                    stats["zorder_columns"] = zorder_cols

            # Get stats after optimization
            details_after = self.spark.sql(
                f"DESCRIBE DETAIL delta.`{table_path_str}`"
            ).collect()[0]
            files_after = details_after["numFiles"]
            size_after_mb = round(details_after["sizeInBytes"] / (1024**2), 2)

            # Run VACUUM
            self.spark.sql(f"VACUUM delta.`{table_path_str}` RETAIN 168 HOURS")

            duration = (datetime.now() - optimization_start).total_seconds()

            stats.update(
                {
                    "files_before": files_before,
                    "files_after": files_after,
                    "size_before_mb": size_before_mb,
                    "size_after_mb": size_after_mb,
                    "duration_seconds": int(duration),
                    "status": "SUCCESS",
                }
            )

            # Log to PostgreSQL
            if self.enable_zorder and stats.get("zorder_columns"):
                log_data = {
                    "table_name": table_name,
                    "zorder_columns": stats["zorder_columns"],
                    "optimization_started": optimization_start,
                    "optimization_completed": datetime.now(),
                    "files_before": files_before,
                    "files_after": files_after,
                    "size_before_mb": size_before_mb,
                    "size_after_mb": size_after_mb,
                    "duration_seconds": int(duration),
                    "status": "SUCCESS",
                }
                self._log_to_postgres("zorder_optimization_log", log_data)

            return stats

        except Exception as e:
            logging.error(f"Optimization failed for {table_name}: {e}")
            stats["status"] = "FAILED"
            stats["error"] = str(e)
            return stats

    def _log_delta_table_stats(self, table_name: str, table_path: Path):
        """Log comprehensive Delta table statistics"""
        try:
            table_path_str = str(table_path)

            # Get table details
            details = self.spark.sql(
                f"DESCRIBE DETAIL delta.`{table_path_str}`"
            ).collect()[0]

            # Get record counts
            total_records = self.spark.sql(
                f"SELECT COUNT(*) as cnt FROM delta.`{table_path_str}`"
            ).collect()[0]["cnt"]

            # SCD2-specific counts
            if self.enable_scd2:
                current_records = self.spark.sql(
                    f"""
                    SELECT COUNT(*) as cnt FROM delta.`{table_path_str}`
                    WHERE _is_current = true
                """
                ).collect()[0]["cnt"]
                historical_records = total_records - current_records
            else:
                current_records = total_records
                historical_records = 0

            # Calculate quality score
            if self.enable_quality_columns:
                quality_result = self.spark.sql(
                    f"""
                    SELECT AVG(_dq_quality_score) as avg_score 
                    FROM delta.`{table_path_str}`
                """
                ).collect()[0]
                avg_quality = (
                    float(quality_result["avg_score"])
                    if quality_result["avg_score"]
                    else 0.0
                )
            else:
                avg_quality = 0.0

            # Get Z-ORDER columns from latest optimization
            try:
                conn = self._get_postgres_connection()
                cur = conn.cursor()
                cur.execute(
                    f"""
                    SELECT zorder_columns FROM {self.postgres_config['schema']}.zorder_optimization_log
                    WHERE table_name = %s AND status = 'SUCCESS'
                    ORDER BY optimization_completed DESC
                    LIMIT 1
                """,
                    (table_name,),
                )
                result = cur.fetchone()
                zorder_cols = result[0] if result else []
                cur.close()
            except:
                zorder_cols = []

            stats_data = {
                "table_name": table_name,
                "total_records": total_records,
                "current_records": current_records,
                "historical_records": historical_records,
                "total_files": details["numFiles"],
                "table_size_mb": round(details["sizeInBytes"] / (1024**2), 2),
                "last_updated": datetime.now(),
                "partition_columns": (
                    details["partitionColumns"] if details["partitionColumns"] else []
                ),
                "zorder_columns": zorder_cols,
                "avg_quality_score": avg_quality,
                "scd2_enabled": self.enable_scd2,
            }

            # UPSERT to PostgreSQL
            conn = self._get_postgres_connection()
            cur = conn.cursor()

            cur.execute(
                f"""
                INSERT INTO {self.postgres_config['schema']}.delta_table_stats 
                (table_name, total_records, current_records, historical_records, total_files, 
                 table_size_mb, last_updated, partition_columns, zorder_columns, avg_quality_score, scd2_enabled)
                VALUES (%(table_name)s, %(total_records)s, %(current_records)s, %(historical_records)s,
                        %(total_files)s, %(table_size_mb)s, %(last_updated)s, %(partition_columns)s, 
                        %(zorder_columns)s, %(avg_quality_score)s, %(scd2_enabled)s)
                ON CONFLICT (table_name) 
                DO UPDATE SET
                    total_records = EXCLUDED.total_records,
                    current_records = EXCLUDED.current_records,
                    historical_records = EXCLUDED.historical_records,
                    total_files = EXCLUDED.total_files,
                    table_size_mb = EXCLUDED.table_size_mb,
                    last_updated = EXCLUDED.last_updated,
                    partition_columns = EXCLUDED.partition_columns,
                    zorder_columns = EXCLUDED.zorder_columns,
                    avg_quality_score = EXCLUDED.avg_quality_score,
                    scd2_enabled = EXCLUDED.scd2_enabled
            """,
                stats_data,
            )

            conn.commit()
            cur.close()

        except Exception as e:
            logging.error(f"Error logging Delta table stats: {e}")

    def _log_batch_distribution(self, table_name: str, operation_id: int):
        """Log batch distribution analytics"""
        try:
            table_path = self.bronze_base_path / "tables" / table_name
            table_path_str = str(table_path)

            if not DeltaTable.isDeltaTable(self.spark, table_path_str):
                return

            # Get batch distribution
            batch_stats = self.spark.sql(
                f"""
                SELECT 
                    _operation_batch_id,
                    DATE(_load_timestamp) as batch_date,
                    COUNT(*) as records_loaded,
                    AVG(_dq_quality_score) as avg_quality_score
                FROM delta.`{table_path_str}`
                WHERE _operation_batch_id = {operation_id}
                GROUP BY _operation_batch_id, DATE(_load_timestamp)
            """
            ).collect()

            for row in batch_stats:
                log_data = {
                    "table_name": table_name,
                    "operation_id": operation_id,
                    "batch_date": row["batch_date"],
                    "records_loaded": row["records_loaded"],
                    "records_updated": 0,  # Would need more complex query
                    "records_inserted": row["records_loaded"],
                    "avg_quality_score": (
                        float(row["avg_quality_score"])
                        if row["avg_quality_score"]
                        else 0.0
                    ),
                }
                self._log_to_postgres("batch_distribution", log_data)

        except Exception as e:
            logging.error(f"Error logging batch distribution: {e}")

    def load_file_incrementally(
        self, file_path: Path, operation_id: int
    ) -> Tuple[bool, int]:
        """Load a single parquet file with full feature support"""
        start_time = datetime.now()

        try:
            # Calculate file hash for incremental loading
            file_hash = self._calculate_file_hash(file_path)

            # Check if already processed (TRUE INCREMENTAL)
            if self._is_file_already_processed(file_path, file_hash):
                logging.info(f"⏭️  Skipping already processed: {file_path.name}")
                return True, 0  # Return True but 0 rows (skipped)

            table_name = self.get_table_name_from_path(file_path)

            logging.info(f"📄 Processing: {file_path.name} -> {table_name}")

            # Read parquet file
            df = self.spark.read.parquet(str(file_path))
            initial_count = df.count()

            # Enhance with metadata
            enhanced_df = self._enhance_with_metadata(df, file_path, operation_id)

            # Apply data quality checks
            quality_df = self._apply_data_quality_checks(enhanced_df, table_name)

            # Calculate quality score for logging
            if self.enable_quality_columns:
                quality_score = quality_df.agg(avg("_dq_quality_score")).collect()[0][0]
                quality_score = float(quality_score) if quality_score else 0.0
            else:
                quality_score = 1.0

            # Write to Delta with SCD2 support
            success, write_stats = self._write_to_delta_table(
                quality_df, table_name, operation_id
            )

            processing_time = (datetime.now() - start_time).total_seconds()

            if success:
                # Log file processing
                file_stats = {
                    "operation_id": operation_id,
                    "file_path": str(file_path),
                    "file_hash": file_hash,
                    "file_size": file_path.stat().st_size,
                    "file_modified_time": datetime.fromtimestamp(
                        file_path.stat().st_mtime
                    ),
                    "records_count": initial_count,
                    "processed_at": datetime.now(),
                    "status": "SUCCESS",
                    "data_quality_score": quality_score,
                    "processing_time_seconds": round(processing_time, 2),
                }
                self._log_to_postgres("file_processing", file_stats)

                # Log table stats
                table_path = self.bronze_base_path / "tables" / table_name
                self._log_delta_table_stats(table_name, table_path)

                # Log batch distribution
                if self.enable_quality_columns:
                    self._log_batch_distribution(table_name, operation_id)

                logging.info(
                    f"  ✓ {initial_count:,} rows, quality: {quality_score:.2f}, time: {processing_time:.2f}s"
                )
                return True, initial_count
            else:
                raise Exception(write_stats.get("error", "Unknown error"))

        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()

            # Log failure
            file_stats = {
                "operation_id": operation_id,
                "file_path": str(file_path),
                "file_hash": file_hash if "file_hash" in locals() else "unknown",
                "file_size": file_path.stat().st_size if file_path.exists() else 0,
                "processed_at": datetime.now(),
                "status": "FAILED",
                "error_message": str(e)[:500],
                "processing_time_seconds": round(processing_time, 2),
            }
            self._log_to_postgres("file_processing", file_stats)

            logging.error(f"  ✗ Error: {e}")
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
            "scd2_enabled": self.enable_scd2,
            "quality_checks_enabled": self.enable_quality_columns,
        }

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
                        "files_skipped": 0,
                        "rows_loaded": 0,
                        "completed_at": datetime.now(),
                        "total_duration_seconds": 0,
                    },
                )
                return {"feature": feature, "status": "NO_FILES", "files_processed": 0}

            files_processed = 0
            files_skipped = 0
            total_rows_loaded = 0
            failed_files = []

            # Process each file
            for file_path in all_files:
                success, rows_loaded = self.load_file_incrementally(
                    file_path, operation_id
                )
                if success:
                    if rows_loaded > 0:
                        files_processed += 1
                        total_rows_loaded += rows_loaded
                    else:
                        files_skipped += 1  # Already processed
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
                    "files_skipped": files_skipped,
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
                "files_skipped": files_skipped,
                "total_files": len(all_files),
                "failed_files": len(failed_files),
                "total_rows_loaded": total_rows_loaded,
                "status": status,
                "duration_seconds": round(duration, 2),
            }

        except Exception as e:
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
            logging.info(f"📦 LOADING FEATURE: {feature.upper()}")
            logging.info(f"{'='*80}")

            result = self.load_feature_incrementally(feature)
            results.append(result)

            if result["status"] in ["COMPLETED", "PARTIAL_SUCCESS"]:
                logging.info(
                    f"✅ {feature}: {result['files_processed']} processed, "
                    f"{result['files_skipped']} skipped, "
                    f"{result['total_rows_loaded']:,} rows, {result['duration_seconds']:.2f}s"
                )
                if result.get("failed_files", 0) > 0:
                    logging.warning(f"  ⚠️  {result['failed_files']} files failed")
            else:
                logging.error(f"❌ {feature}: {result.get('error', 'Unknown error')}")

        return results

    def optimize_all_tables(self):
        """Optimize all Delta tables with Z-ORDER"""
        logging.info("\n" + "=" * 80)
        logging.info("⚡ OPTIMIZING DELTA TABLES")
        logging.info("=" * 80)

        tables_path = self.bronze_base_path / "tables"
        if not tables_path.exists():
            logging.warning("No tables found to optimize")
            return

        for table_dir in sorted(tables_path.iterdir()):
            if table_dir.is_dir() and DeltaTable.isDeltaTable(
                self.spark, str(table_dir)
            ):
                logging.info(f"\n🔧 Optimizing {table_dir.name}...")
                stats = self._optimize_delta_table(table_dir, table_dir.name)

                if stats.get("status") == "SUCCESS":
                    logging.info(
                        f"  Files: {stats['files_before']} → {stats['files_after']}"
                    )
                    logging.info(
                        f"  Size: {stats['size_before_mb']} MB → {stats['size_after_mb']} MB"
                    )
                    logging.info(f"  Duration: {stats['duration_seconds']}s")
                    if stats.get("zorder_columns"):
                        logging.info(f"  Z-ORDER: {', '.join(stats['zorder_columns'])}")

    def get_batch_distribution_report(self, table_name: str = None) -> List[Dict]:
        """Get batch distribution analytics"""
        try:
            conn = self._get_postgres_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)

            if table_name:
                query = f"""
                    SELECT 
                        table_name,
                        batch_date,
                        records_loaded,
                        avg_quality_score,
                        created_at
                    FROM {self.postgres_config['schema']}.batch_distribution
                    WHERE table_name = %s
                    ORDER BY batch_date DESC
                    LIMIT 100
                """
                cur.execute(query, (table_name,))
            else:
                query = f"""
                    SELECT 
                        table_name,
                        batch_date,
                        SUM(records_loaded) as total_records,
                        AVG(avg_quality_score) as avg_quality,
                        COUNT(*) as batch_count
                    FROM {self.postgres_config['schema']}.batch_distribution
                    GROUP BY table_name, batch_date
                    ORDER BY batch_date DESC, table_name
                    LIMIT 100
                """
                cur.execute(query)

            results = [dict(row) for row in cur.fetchall()]
            cur.close()

            return results

        except Exception as e:
            logging.error(f"Error getting batch distribution: {e}")
            return []

    def get_schema_evolution_report(self) -> List[Dict]:
        """Get schema evolution history"""
        try:
            conn = self._get_postgres_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)

            cur.execute(
                f"""
                SELECT 
                    table_name,
                    change_type,
                    column_name,
                    old_data_type,
                    new_data_type,
                    change_date,
                    resolved
                FROM {self.postgres_config['schema']}.schema_evolution
                ORDER BY change_date DESC
                LIMIT 50
            """
            )

            results = [dict(row) for row in cur.fetchall()]
            cur.close()

            return results

        except Exception as e:
            logging.error(f"Error getting schema evolution: {e}")
            return []

    def get_summary_report(self) -> Dict:
        """Get comprehensive summary report"""
        try:
            conn = self._get_postgres_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)

            # Overall stats
            cur.execute(
                f"""
                SELECT 
                    COUNT(*) as total_operations,
                    SUM(files_processed) as total_files_processed,
                    SUM(files_skipped) as total_files_skipped,
                    SUM(rows_loaded) as total_rows,
                    AVG(total_duration_seconds) as avg_duration,
                    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as successful_operations,
                    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_operations
                FROM {self.postgres_config['schema']}.load_operations
            """
            )

            overall_stats = dict(cur.fetchone())

            # Feature breakdown
            cur.execute(
                f"""
                SELECT 
                    feature_name,
                    COUNT(*) as operations,
                    SUM(files_processed) as files_processed,
                    SUM(files_skipped) as files_skipped,
                    SUM(rows_loaded) as rows,
                    MAX(completed_at) as last_run,
                    BOOL_OR(scd2_enabled) as scd2_enabled
                FROM {self.postgres_config['schema']}.load_operations
                GROUP BY feature_name
                ORDER BY feature_name
            """
            )

            feature_stats = [dict(row) for row in cur.fetchall()]

            # Table stats
            cur.execute(
                f"""
                SELECT 
                    table_name,
                    total_records,
                    current_records,
                    historical_records,
                    total_files,
                    table_size_mb,
                    avg_quality_score,
                    last_updated,
                    scd2_enabled,
                    zorder_columns
                FROM {self.postgres_config['schema']}.delta_table_stats
                ORDER BY total_records DESC
            """
            )

            table_stats = [dict(row) for row in cur.fetchall()]

            # Schema evolution count
            cur.execute(
                f"""
                SELECT COUNT(*) as schema_changes
                FROM {self.postgres_config['schema']}.schema_evolution
                WHERE resolved = false
            """
            )

            schema_changes = cur.fetchone()["schema_changes"]

            cur.close()

            return {
                "overall": overall_stats,
                "by_feature": feature_stats,
                "tables": table_stats,
                "unresolved_schema_changes": schema_changes,
            }

        except Exception as e:
            logging.error(f"Error generating summary report: {e}")
            return {}

    def print_summary_report(self):
        """Print comprehensive formatted summary report"""
        report = self.get_summary_report()

        if not report:
            logging.warning("No report data available")
            return

        print("\n" + "=" * 80)
        print("📊 PRODUCTION BRONZE LAYER - COMPREHENSIVE REPORT")
        print("=" * 80)

        # Overall stats
        overall = report.get("overall", {})
        print(f"\n🎯 OVERALL STATISTICS:")
        print(f"  Total Operations: {overall.get('total_operations', 0)}")
        print(f"  ✅ Successful: {overall.get('successful_operations', 0)}")
        print(f"  ❌ Failed: {overall.get('failed_operations', 0)}")
        print(f"  📁 Files Processed: {overall.get('total_files_processed', 0):,}")
        print(
            f"  ⏭️  Files Skipped (Already Loaded): {overall.get('total_files_skipped', 0):,}"
        )
        print(f"  📊 Total Rows Loaded: {overall.get('total_rows', 0):,}")
        print(f"  ⏱️  Average Duration: {overall.get('avg_duration', 0):.2f}s")

        # Feature breakdown
        print(f"\n📁 BY FEATURE:")
        for feature in report.get("by_feature", []):
            scd2_status = "🔄 SCD2" if feature.get("scd2_enabled") else "📝 Simple"
            print(f"  {feature['feature_name'].upper()} ({scd2_status}):")
            print(f"    Operations: {feature['operations']}")
            print(f"    Files Processed: {feature['files_processed']:,}")
            print(f"    Files Skipped: {feature['files_skipped']:,}")
            print(f"    Rows: {feature['rows']:,}")
            print(f"    Last Run: {feature['last_run']}")

        # Table stats
        print(f"\n📋 DELTA TABLES:")
        for table in report.get("tables", []):
            scd2_badge = "🔄" if table.get("scd2_enabled") else "📝"
            print(f"  {scd2_badge} {table['table_name']}:")
            print(f"    Total Records: {table['total_records']:,}")
            if table.get("scd2_enabled"):
                print(f"    ├─ Current: {table['current_records']:,}")
                print(f"    └─ Historical: {table['historical_records']:,}")
            print(f"    Files: {table['total_files']}")
            print(f"    Size: {table['table_size_mb']:.2f} MB")
            print(f"    Quality Score: {table['avg_quality_score']:.2f}")
            if table.get("zorder_columns"):
                print(f"    Z-ORDER: {', '.join(table['zorder_columns'])}")
            print(f"    Last Updated: {table['last_updated']}")

        # Schema changes
        schema_changes = report.get("unresolved_schema_changes", 0)
        if schema_changes > 0:
            print(f"\n⚠️  SCHEMA EVOLUTION:")
            print(f"  Unresolved Changes: {schema_changes}")
            print(f"  Run get_schema_evolution_report() for details")
        else:
            print(f"\n✅ No unresolved schema changes")

        print("\n" + "=" * 80)

    def verify_loaded_data(self):
        """Verify loaded data with batch distribution"""
        tables_path = self.bronze_base_path / "tables"

        if not tables_path.exists():
            logging.warning("No tables directory found")
            return []

        logging.info("\n" + "=" * 80)
        logging.info("🔍 VERIFYING DELTA TABLES")
        logging.info("=" * 80)

        tables = []
        for table_dir in sorted(tables_path.iterdir()):
            if table_dir.is_dir() and DeltaTable.isDeltaTable(
                self.spark, str(table_dir)
            ):
                try:
                    table_name = table_dir.name
                    tables.append(table_name)
                    table_path_str = str(table_dir)

                    # Get record counts
                    total_count = self.spark.sql(
                        f"SELECT COUNT(*) as cnt FROM delta.`{table_path_str}`"
                    ).collect()[0]["cnt"]

                    print(f"\n📊 {table_name}")
                    print(f"  ├─ Total Records: {total_count:,}")

                    # SCD2 breakdown
                    if self.enable_scd2:
                        current_count = self.spark.sql(
                            f"""
                            SELECT COUNT(*) as cnt FROM delta.`{table_path_str}`
                            WHERE _is_current = true
                        """
                        ).collect()[0]["cnt"]
                        historical_count = total_count - current_count
                        print(f"  ├─ Current Records: {current_count:,}")
                        print(f"  ├─ Historical Records: {historical_count:,}")

                    # Batch distribution
                    batch_dist = self.spark.sql(
                        f"""
                        SELECT 
                            _operation_batch_id,
                            COUNT(*) as records,
                            MIN(_load_timestamp) as first_load,
                            MAX(_load_timestamp) as last_load
                        FROM delta.`{table_path_str}`
                        GROUP BY _operation_batch_id
                        ORDER BY _operation_batch_id DESC
                        LIMIT 5
                    """
                    ).collect()

                    if batch_dist:
                        print(f"  └─ Recent Batches:")
                        for batch in batch_dist:
                            print(
                                f"      Batch {batch['_operation_batch_id']}: {batch['records']:,} records ({batch['first_load']} - {batch['last_load']})"
                            )

                except Exception as e:
                    logging.error(f"Error verifying table {table_dir.name}: {e}")

        print("\n" + "=" * 80)
        return tables

    def create_bronze_catalog(self):
        """Create Spark SQL database and catalog entries"""
        try:
            self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze_layer")

            tables_path = self.bronze_base_path / "tables"
            if not tables_path.exists():
                logging.warning("No tables to catalog")
                return

            logging.info("\n" + "=" * 80)
            logging.info("📚 CREATING BRONZE CATALOG")
            logging.info("=" * 80)

            for table_dir in tables_path.iterdir():
                if table_dir.is_dir() and DeltaTable.isDeltaTable(
                    self.spark, str(table_dir)
                ):
                    table_name = table_dir.name

                    self.spark.sql(f"DROP TABLE IF EXISTS bronze_layer.{table_name}")

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

    def print_feature_flags(self):
        """Print enabled features"""
        print("\n" + "=" * 80)
        print("🎛️  ENABLED FEATURES")
        print("=" * 80)
        print(
            f"  {'✅' if self.enable_scd2 else '❌'} SCD Type 2 (Historical Tracking)"
        )
        print(
            f"  {'✅' if self.enable_quality_columns else '❌'} Per-Column Quality Checks"
        )
        print(f"  {'✅' if self.enable_zorder else '❌'} Z-ORDER Optimization")
        print(f"  ✅ Schema Evolution Tracking (Always On)")
        print(f"  ✅ Incremental File Loading (Always On)")
        print(f"  ✅ Batch Distribution Analytics (Always On)")
        print("=" * 80)

    def cleanup(self):
        """Cleanup resources"""
        try:
            if self.pg_conn and not self.pg_conn.closed:
                self.pg_conn.close()
                logging.info("✓ PostgreSQL connection closed")

            if self.spark:
                self.spark.stop()
                logging.info("✓ Spark session stopped")

        except Exception as e:
            logging.error(f"Error during cleanup: {e}")


def main():
    """Main execution function with all production features"""
    log_dir = Path("/opt/etl/logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "bronze_loader_production.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logger = logging.getLogger(__name__)
    loader = None

    try:
        logger.info("=" * 80)
        logger.info("🚀 PRODUCTION BRONZE LAYER LOADER - ALL FEATURES ENABLED")
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

        # Spark configurations
        spark_configs = {
            "spark.sql.shuffle.partitions": "8",
            "spark.driver.memory": "4g",
            "spark.executor.memory": "4g",
        }

        # Initialize loader with ALL features enabled
        loader = ProductionBronzeLoader(
            bronze_base_path=bronze_base_path,
            source_data_path=source_data_path,
            postgres_config=postgres_config,
            spark_configs=spark_configs,
            enable_scd2=True,  # ✅ Historical tracking
            enable_quality_columns=True,  # ✅ Per-column quality flags
            enable_zorder=True,  # ✅ Z-ORDER optimization
        )

        # Print enabled features
        loader.print_feature_flags()

        # Load all features
        logger.info("\n🚀 Starting incremental load with full feature support...")
        results = loader.load_all_features()

        # Optimize tables with Z-ORDER
        logger.info("\n⚡ Optimizing Delta tables with Z-ORDER...")
        loader.optimize_all_tables()

        # Create catalog
        logger.info("\n📚 Creating Spark catalog...")
        loader.create_bronze_catalog()

        # Verify data with batch distribution
        logger.info("\n🔍 Verifying loaded data...")
        loader.verify_loaded_data()

        # Print comprehensive summary report
        loader.print_summary_report()

        # Print schema evolution if any
        schema_changes = loader.get_schema_evolution_report()
        if schema_changes:
            print("\n" + "=" * 80)
            print("📋 SCHEMA EVOLUTION HISTORY")
            print("=" * 80)
            for change in schema_changes[:10]:  # Show last 10
                status = "✅" if change["resolved"] else "⚠️"
                print(f"{status} {change['table_name']}: {change['change_type']}")
                print(f"   Column: {change['column_name']}")
                if change["old_data_type"]:
                    print(
                        f"   Type: {change['old_data_type']} → {change['new_data_type']}"
                    )
                print(f"   Date: {change['change_date']}")

        # Print batch distribution sample
        batch_dist = loader.get_batch_distribution_report()
        if batch_dist:
            print("\n" + "=" * 80)
            print("📊 BATCH DISTRIBUTION ANALYTICS (Last 10)")
            print("=" * 80)
            for batch in batch_dist[:10]:
                print(f"  {batch['table_name']}")
                print(f"    Date: {batch['batch_date']}")
                print(
                    f"    Records: {batch.get('total_records', batch.get('records_loaded', 0)):,}"
                )
                print(f"    Quality: {batch.get('avg_quality', 0):.2f}")

        # Check for failures
        failed = [r for r in results if r["status"] == "FAILED"]
        partial = [r for r in results if r["status"] == "PARTIAL_SUCCESS"]

        print("\n" + "=" * 80)
        if failed:
            logger.error(f"❌ {len(failed)} feature(s) failed completely")
            return False
        elif partial:
            logger.warning(f"⚠️  {len(partial)} feature(s) had partial failures")
            return True
        else:
            logger.info("✅ ALL FEATURES LOADED SUCCESSFULLY!")
            logger.info("\n📊 Production Features Active:")
            logger.info("  ✅ SCD Type 2 - Historical records preserved")
            logger.info("  ✅ Schema Evolution - All changes tracked")
            logger.info("  ✅ Incremental Loading - Skip already processed files")
            logger.info("  ✅ Z-ORDER Optimization - Query performance optimized")
            logger.info("  ✅ Quality Checks - Per-column quality tracking")
            logger.info("  ✅ Batch Analytics - Complete observability")
            return True

    except Exception as e:
        logger.error(f"\n❌ Bronze layer load failed: {e}", exc_info=True)
        return False

    finally:
        if loader:
            loader.cleanup()
        print("\n" + "=" * 80)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

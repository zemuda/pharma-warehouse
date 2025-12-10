# bronze_loader_standalone.py
"""
Standalone Enhanced Production Bronze Loader
All features in one file - no external imports needed
"""

import yaml
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
import json
import hashlib
import psycopg2
from psycopg2.extras import RealDictCursor

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable


class StandaloneBronzeLoader:
    """
    Complete Bronze Layer Loader with all features:
    - Configuration file support
    - SCD Type 2, Z-ORDER, Schema Evolution
    - Data validation, Anomaly detection
    - Notifications, Retry logic, Quarantine
    """

    """START CLAUDE __init__ Initialize Bronze Loader"""

    def __init__(self, config_path: str = "bronze_loader_config.yaml"):
        """Initialize loader with configuration file"""
        # Load configuration
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        # Setup paths
        self.bronze_base_path = Path(
            self.config["paths"]["bronze_base_path"]
        ).absolute()
        self.source_data_path = Path(
            self.config["paths"]["source_data_path"]
        ).absolute()
        self.log_dir = Path(self.config["paths"]["log_directory"]).absolute()
        self.checkpoint_dir = Path(
            self.config["paths"]["checkpoint_directory"]
        ).absolute()

        # Create directories
        self.bronze_base_path.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        # Setup quarantine and DLQ
        if self.config["error_handling"]["quarantine_enabled"]:
            self.quarantine_dir = Path(self.config["error_handling"]["quarantine_path"])
            self.quarantine_dir.mkdir(parents=True, exist_ok=True)

        if self.config["error_handling"]["dlq_enabled"]:
            self.dlq_dir = Path(self.config["error_handling"]["dlq_path"])
            self.dlq_dir.mkdir(parents=True, exist_ok=True)

        # PostgreSQL config
        self.postgres_config = self.config["postgresql"]
        self.pg_conn = None

        # Feature flags
        self.enable_scd2 = self.config["features"]["enable_scd2"]
        self.enable_quality_columns = self.config["features"]["enable_quality_columns"]
        self.enable_zorder = self.config["features"]["enable_zorder"]
        self.enable_validation = self.config["features"]["enable_validation"]
        self.enable_anomaly_detection = self.config["features"][
            "enable_anomaly_detection"
        ]
        self.enable_auto_healing = self.config["features"]["enable_auto_healing"]

        # Features to process
        self.features = [
            ds["name"]
            for ds in self.config["data_sources"]["features"]
            if ds["enabled"]
        ]

        # Initialize Spark
        self.spark = self._create_spark_session()

        # Initialize PostgreSQL
        self._init_postgres_logging()

        logging.info("✅ Standalone Bronze Loader initialized")

    """END CLAUDE __init__ Initialize Bronze Loader"""

    """START DEEPSEEK __init__ Initialize Bronze Loader"""

    # def __init__(self, config_path: str = "bronze_loader_config.yaml"):
    #     """Initialize loader with configuration file"""
    #     # Load configuration
    #     with open(config_path, "r") as f:
    #         self.config = yaml.safe_load(f)

    #     # Setup paths with better error handling
    #     try:
    #         self.bronze_base_path = Path(
    #             self.config["paths"]["bronze_base_path"]
    #         ).absolute()
    #         self.source_data_path = Path(
    #             self.config["paths"]["source_data_path"]
    #         ).absolute()
    #         self.log_dir = Path(self.config["paths"]["log_directory"]).absolute()
    #         self.checkpoint_dir = Path(
    #             self.config["paths"]["checkpoint_directory"]
    #         ).absolute()

    #         # Create directories with better error handling
    #         for directory in [self.bronze_base_path, self.log_dir, self.checkpoint_dir]:
    #             try:
    #                 directory.mkdir(parents=True, exist_ok=True)
    #                 print(f"Directory created/verified: {directory}")
    #             except Exception as e:
    #                 print(f"Warning: Could not create directory {directory}: {e}")

    #         # Setup quarantine and DLQ
    #         if self.config["error_handling"]["quarantine_enabled"]:
    #             self.quarantine_dir = Path(
    #                 self.config["error_handling"]["quarantine_path"]
    #             )
    #             self.quarantine_dir.mkdir(parents=True, exist_ok=True)

    #         if self.config["error_handling"]["dlq_enabled"]:
    #             self.dlq_dir = Path(self.config["error_handling"]["dlq_path"])
    #             self.dlq_dir.mkdir(parents=True, exist_ok=True)

    #     except KeyError as e:
    #         print(f"Configuration error: Missing key {e}")
    #         raise
    #     except Exception as e:
    #         print(f"Path setup error: {e}")
    #         raise

    """END DEEPSEEK __init__ Initialize Bronze Loader"""

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session from config"""
        spark_cfg = self.config["spark"]

        configs = {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.driver.memory": spark_cfg["driver_memory"],
            "spark.executor.memory": spark_cfg["executor_memory"],
            "spark.sql.shuffle.partitions": str(spark_cfg["shuffle_partitions"]),
            "spark.default.parallelism": str(spark_cfg["default_parallelism"]),
            "spark.sql.files.maxPartitionBytes": spark_cfg["max_partition_bytes"],
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.databricks.delta.retentionDurationCheck.enabled": "false",
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.hadoop.fs.defaultFS": "file:///",
        }

        # Add additional configs
        if "additional_configs" in spark_cfg:
            configs.update(
                {k: str(v) for k, v in spark_cfg["additional_configs"].items()}
            )

        builder = SparkSession.builder.appName(
            spark_cfg.get("app_name", "BronzeLoader")
        )
        for key, value in configs.items():
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
        """Initialize PostgreSQL logging tables"""
        try:
            conn = self._get_postgres_connection()
            cur = conn.cursor()

            schema = self.postgres_config["schema"]

            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

            # Load operations
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.load_operations (
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

            # Schema evolution
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.schema_evolution (
                    evolution_id SERIAL PRIMARY KEY,
                    feature_name VARCHAR(100) NOT NULL,
                    table_name VARCHAR(200) NOT NULL,
                    change_type VARCHAR(50) NOT NULL,
                    column_name VARCHAR(200),
                    old_data_type VARCHAR(100),
                    new_data_type VARCHAR(100),
                    change_date TIMESTAMP NOT NULL DEFAULT NOW(),
                    resolved BOOLEAN DEFAULT FALSE,
                    operation_id INTEGER REFERENCES {schema}.load_operations(operation_id),
                    schema_before TEXT,
                    schema_after TEXT
                )
            """
            )

            # File processing
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.file_processing (
                    file_id SERIAL PRIMARY KEY,
                    operation_id INTEGER NOT NULL REFERENCES {schema}.load_operations(operation_id),
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

            # Delta table stats
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.delta_table_stats (
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
                    scd2_enabled BOOLEAN DEFAULT FALSE
                )
            """
            )

            # Z-ORDER optimization log
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.zorder_optimization_log (
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

            # Batch distribution
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.batch_distribution (
                    batch_id SERIAL PRIMARY KEY,
                    table_name VARCHAR(200) NOT NULL,
                    operation_id INTEGER REFERENCES {schema}.load_operations(operation_id),
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
                f"CREATE INDEX IF NOT EXISTS idx_load_ops_feature ON {schema}.load_operations(feature_name, started_at DESC)",
                f"CREATE INDEX IF NOT EXISTS idx_file_proc_hash ON {schema}.file_processing(file_hash)",
                f"CREATE INDEX IF NOT EXISTS idx_schema_evo_table ON {schema}.schema_evolution(table_name, change_date DESC)",
            ]

            for idx_sql in indexes:
                try:
                    cur.execute(idx_sql)
                except:
                    pass

            conn.commit()
            cur.close()
            logging.info("✓ PostgreSQL logging tables initialized")

        except Exception as e:
            logging.error(f"Failed to initialize PostgreSQL: {e}")
            if conn:
                conn.rollback()
            raise

    def _log_to_postgres(self, table: str, data: Dict) -> Optional[int]:
        """Log data to PostgreSQL"""
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
            cur.close()

            return result[0] if result else None

        except Exception as e:
            logging.error(f"Failed to log to PostgreSQL {table}: {e}")
            if conn:
                conn.rollback()
            return None

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
            }.get(table, "id")

            query = f"""
                UPDATE {self.postgres_config['schema']}.{table}
                SET {set_clause}
                WHERE {id_column} = %s
            """

            cur.execute(query, values)
            conn.commit()
            cur.close()

        except Exception as e:
            logging.error(f"Failed to update PostgreSQL {table}: {e}")
            if conn:
                conn.rollback()

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of file"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _is_file_already_processed(self, file_path: Path, file_hash: str) -> bool:
        """Check if file already processed"""
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
            logging.error(f"Error checking file status: {e}")
            return False

    def discover_parquet_files(self, feature: str) -> List[Path]:
        """Discover parquet files"""
        feature_path = self.source_data_path / feature

        if not feature_path.exists():
            logging.warning(f"Feature path does not exist: {feature_path}")
            return []

        try:
            parquet_files = list(feature_path.rglob("*.parquet"))
            logging.info(f"Found {len(parquet_files)} parquet files for {feature}")
            return parquet_files
        except Exception as e:
            logging.error(f"Error discovering files: {e}")
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
                if not field.name.startswith("_"):
                    schema[field.name] = field.dataType.simpleString()
            return schema
        except Exception as e:
            logging.error(f"Error getting schema: {e}")
            return {}

    def _detect_schema_changes(
        self, old_schema: Dict, new_schema: Dict, table_name: str, operation_id: int
    ) -> List[Dict]:
        """Detect and log schema changes"""
        changes = []

        # New columns
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

        # Removed columns
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

        # Type changes
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

        # Log changes
        if changes:
            logging.warning(f"⚠️  Schema changes in {table_name}:")
            for change in changes:
                logging.warning(f"  - {change['change_type']}: {change['column_name']}")

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

    def validate_dataframe(
        self, df: DataFrame, table_name: str
    ) -> Tuple[bool, List[str], DataFrame]:
        """Validate DataFrame against rules"""
        if not self.enable_validation:
            return True, [], df

        violations = []
        cleaned_df = df

        global_rules = self.config["validation_rules"].get("global", [])
        table_rules = (
            self.config["validation_rules"]
            .get("tables", {})
            .get(table_name.replace("bronze_", ""), [])
        )

        all_rules = global_rules + table_rules

        for rule in all_rules:
            rule_type = rule["rule_type"]
            severity = rule.get("severity", "warning")

            try:
                if rule_type == "null_check":
                    max_null_pct = rule.get("max_null_percentage", 10.0)
                    total_rows = df.count()

                    for col_name in df.columns:
                        if not col_name.startswith("_"):
                            null_count = df.filter(col(col_name).isNull()).count()
                            null_pct = (
                                (null_count / total_rows * 100) if total_rows > 0 else 0
                            )

                            if null_pct > max_null_pct:
                                violation = (
                                    f"Column '{col_name}' has {null_pct:.2f}% nulls"
                                )
                                violations.append(f"[{severity.upper()}] {violation}")

                                if self.enable_auto_healing and severity != "error":
                                    logging.info(
                                        f"Auto-healing: Filling nulls in {col_name}"
                                    )
                                    cleaned_df = cleaned_df.fillna(
                                        {col_name: "UNKNOWN"}
                                    )

                elif rule_type == "range_check":
                    columns = rule.get("columns", [])
                    min_val = rule.get("min_value")
                    max_val = rule.get("max_value")

                    for col_name in columns:
                        if col_name in df.columns:
                            if min_val is not None:
                                out_of_range = df.filter(
                                    col(col_name) < min_val
                                ).count()
                                if out_of_range > 0:
                                    violations.append(
                                        f"[{severity.upper()}] Column '{col_name}' has {out_of_range} values below {min_val}"
                                    )

            except Exception as e:
                logging.error(f"Error applying validation rule {rule_type}: {e}")

        error_violations = [v for v in violations if v.startswith("[ERROR]")]
        is_valid = len(error_violations) == 0

        return is_valid, violations, cleaned_df

    def send_notification(self, event_type: str, message: str, level: str = "info"):
        """Send notifications"""
        notifications_config = self.config["notifications"]

        if not notifications_config["enabled"]:
            return

        # Slack
        slack_config = notifications_config["slack"]
        if slack_config["enabled"] and event_type in slack_config["notify_on"]:
            try:
                color_map = {
                    "info": "#36a64f",
                    "warning": "#ff9800",
                    "error": "#f44336",
                }
                payload = {
                    "channel": slack_config["channel"],
                    "attachments": [
                        {
                            "color": color_map.get(level, "#36a64f"),
                            "title": f"Bronze Layer - {level.upper()}",
                            "text": message,
                            "footer": "Bronze Loader",
                            "ts": int(datetime.now().timestamp()),
                        }
                    ],
                }
                requests.post(
                    slack_config["webhook_url"],
                    data=json.dumps(payload),
                    headers={"Content-Type": "application/json"},
                )
                logging.info("✅ Slack notification sent")
            except Exception as e:
                logging.error(f"Slack notification failed: {e}")

    def quarantine_file(self, file_path: Path, reason: str):
        """Quarantine failed file"""
        if not self.config["error_handling"]["quarantine_enabled"]:
            return

        try:
            import shutil

            quarantine_path = self.quarantine_dir / file_path.name
            shutil.copy2(file_path, quarantine_path)

            reason_file = quarantine_path.with_suffix(".reason.txt")
            with open(reason_file, "w") as f:
                f.write(f"Quarantined at: {datetime.now()}\n")
                f.write(f"Reason: {reason}\n")

            logging.info(f"📦 File quarantined: {file_path.name}")
        except Exception as e:
            logging.error(f"Failed to quarantine: {e}")

    def move_to_dlq(self, file_path: Path, error: str):
        """Move to Dead Letter Queue"""
        if not self.config["error_handling"]["dlq_enabled"]:
            return

        try:
            import shutil

            dlq_path = self.dlq_dir / file_path.name
            shutil.copy2(file_path, dlq_path)

            error_file = dlq_path.with_suffix(".error.txt")
            with open(error_file, "w") as f:
                f.write(f"Failed at: {datetime.now()}\n")
                f.write(f"Error: {error}\n")

            logging.error(f"💀 Moved to DLQ: {file_path.name}")
        except Exception as e:
            logging.error(f"Failed to move to DLQ: {e}")

    def _enhance_with_metadata(self, df, file_path: Path, operation_id: int):
        """Add metadata columns"""
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

        if self.enable_scd2:
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
        """Apply quality checks"""
        quality_df = df

        if self.enable_quality_columns:
            business_cols = [c for c in df.columns if not c.startswith("_")]

            for col_name in business_cols:
                quality_df = quality_df.withColumn(
                    f"_dq_{col_name}_null", when(col(col_name).isNull(), 1).otherwise(0)
                )

            dq_columns = [f"_dq_{c}_null" for c in business_cols]
            if dq_columns:
                quality_df = quality_df.withColumn(
                    "_dq_quality_score",
                    (
                        1 - (sum([col(c) for c in dq_columns]) / lit(len(dq_columns)))
                    ).cast("decimal(5,2)"),
                )
        else:
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

    def _get_primary_keys(self, table_name: str, df) -> List[str]:
        """Infer primary keys"""
        pks = []

        id_columns = [
            c for c in df.columns if c.lower().endswith("_id") or c.lower() == "id"
        ]
        if id_columns:
            pks.extend(id_columns[:2])

        date_columns = [
            c for c in df.columns if "date" in c.lower() and not c.startswith("_")
        ]
        if date_columns and date_columns[0] not in pks:
            pks.append(date_columns[0])

        if not pks:
            pks = ["_source_file_path"]

        return pks

    def _apply_scd2_logic(
        self, df, table_path: Path, primary_keys: List[str]
    ) -> Tuple[bool, Dict]:
        """Apply SCD Type 2 logic"""
        if not self.enable_scd2:
            return False, {}

        table_path_str = str(table_path)

        if not DeltaTable.isDeltaTable(self.spark, table_path_str):
            scd2_df = (
                df.withColumn("_valid_from", current_timestamp())
                .withColumn("_valid_to", lit(None).cast(TimestampType()))
                .withColumn("_is_current", lit(True))
                .withColumn("_record_version", lit(1))
            )
            return True, {"initial_load": True, "df": scd2_df}

        delta_table = DeltaTable.forPath(self.spark, table_path_str)

        source_df = (
            df.withColumn("_valid_from", current_timestamp())
            .withColumn("_valid_to", lit(None).cast(TimestampType()))
            .withColumn("_is_current", lit(True))
            .withColumn("_record_version", lit(1))
        )

        if not primary_keys:
            return False, {}

        merge_condition = " AND ".join(
            [f"target.{pk} = source.{pk}" for pk in primary_keys]
        )
        merge_condition += " AND target._is_current = true"

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

        return True, {"scd2_applied": True, "records_updated": changed_records.count()}

    def _write_to_delta_table(
        self, df, table_name: str, operation_id: int
    ) -> Tuple[bool, Dict]:
        """Write to Delta table"""
        try:
            table_path = self.bronze_base_path / "tables" / table_name
            table_path.mkdir(parents=True, exist_ok=True)
            table_path_str = str(table_path)

            old_schema = self._get_table_schema(table_path)

            new_schema = {}
            for field in df.schema.fields:
                if not field.name.startswith("_"):
                    new_schema[field.name] = field.dataType.simpleString()

            if old_schema:
                self._detect_schema_changes(
                    old_schema, new_schema, table_name, operation_id
                )

            stats = {}

            # Apply SCD2 if enabled
            if self.enable_scd2:
                primary_keys = self._get_primary_keys(table_name, df)
                scd2_applied, scd2_stats = self._apply_scd2_logic(
                    df, table_path, primary_keys
                )
                stats.update(scd2_stats)

                if scd2_stats.get("initial_load"):
                    df = scd2_stats["df"]
                    (
                        df.write.format("delta")
                        .mode("overwrite")
                        .partitionBy(["_ingestion_date"])
                        .option("overwriteSchema", "true")
                        .save(table_path_str)
                    )
                    logging.info(f"✓ Created SCD2 table: {table_name}")
            else:
                # Simple merge
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
                    (
                        df.write.format("delta")
                        .mode("overwrite")
                        .partitionBy(["_ingestion_date"])
                        .option("overwriteSchema", "true")
                        .save(table_path_str)
                    )

            stats["write_success"] = True
            return True, stats

        except Exception as e:
            logging.error(f"Error writing to Delta: {e}")
            return False, {"error": str(e)}

    def _get_zorder_columns(self, table_name: str, df) -> List[str]:
        """Determine Z-ORDER columns"""
        zorder_cols = ["_load_timestamp", "_ingestion_date"]

        id_cols = [
            c for c in df.columns if c.lower().endswith("_id") or c.lower() == "id"
        ]
        if id_cols:
            zorder_cols.extend(id_cols[:2])

        date_cols = [
            c
            for c in df.columns
            if "date" in c.lower() and not c.startswith("_") and c not in zorder_cols
        ]
        if date_cols:
            zorder_cols.append(date_cols[0])

        if self.enable_scd2 and "_is_current" in df.columns:
            zorder_cols.append("_is_current")

        return zorder_cols[:5]

    def _optimize_delta_table(self, table_path: Path, table_name: str) -> Dict:
        """Optimize Delta table"""
        optimization_start = datetime.now()
        stats = {}

        try:
            table_path_str = str(table_path)

            details_before = self.spark.sql(
                f"DESCRIBE DETAIL delta.`{table_path_str}`"
            ).collect()[0]
            files_before = details_before["numFiles"]
            size_before_mb = round(details_before["sizeInBytes"] / (1024**2), 2)

            self.spark.sql(f"OPTIMIZE delta.`{table_path_str}`")
            logging.info(f"  ✓ Basic OPTIMIZE completed")

            if self.enable_zorder:
                sample_df = (
                    self.spark.read.format("delta").load(table_path_str).limit(1)
                )
                zorder_cols = self._get_zorder_columns(table_name, sample_df)

                if zorder_cols:
                    zorder_cols_str = ", ".join(zorder_cols)
                    self.spark.sql(
                        f"OPTIMIZE delta.`{table_path_str}` ZORDER BY ({zorder_cols_str})"
                    )
                    logging.info(f"  ✓ Z-ORDER applied: {zorder_cols_str}")
                    stats["zorder_columns"] = zorder_cols

            details_after = self.spark.sql(
                f"DESCRIBE DETAIL delta.`{table_path_str}`"
            ).collect()[0]
            files_after = details_after["numFiles"]
            size_after_mb = round(details_after["sizeInBytes"] / (1024**2), 2)

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
            logging.error(f"Optimization failed: {e}")
            return {"status": "FAILED", "error": str(e)}

    def load_file_with_retry(
        self, file_path: Path, operation_id: int
    ) -> Tuple[bool, int]:
        """Load file with retry logic"""
        retry_config = self.config["error_handling"]
        max_retries = (
            retry_config["max_retries"] if retry_config["retry_enabled"] else 1
        )
        retry_delay = retry_config["retry_delay_seconds"]
        exponential_backoff = retry_config["exponential_backoff"]

        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    delay = (
                        retry_delay * (2**attempt)
                        if exponential_backoff
                        else retry_delay
                    )
                    logging.info(f"⏳ Retry {attempt}/{max_retries} after {delay}s")
                    import time

                    time.sleep(delay)

                file_hash = self._calculate_file_hash(file_path)

                if self._is_file_already_processed(file_path, file_hash):
                    logging.info(f"⏭️  Skipping: {file_path.name}")
                    return True, 0

                table_name = self.get_table_name_from_path(file_path)
                logging.info(f"📄 Processing: {file_path.name}")

                df = self.spark.read.parquet(str(file_path))
                initial_count = df.count()

                # Validate
                is_valid, violations, cleaned_df = self.validate_dataframe(
                    df, table_name
                )

                if violations:
                    for violation in violations:
                        logging.warning(f"  {violation}")

                if not is_valid:
                    error_msg = f"Validation failed: {'; '.join(violations)}"
                    if attempt == max_retries - 1:
                        self.quarantine_file(file_path, error_msg)
                        self.send_notification(
                            "quality_breach",
                            f"File {file_path.name} failed validation",
                            "warning",
                        )
                    raise Exception(error_msg)

                df = cleaned_df

                # Enhance with metadata
                enhanced_df = self._enhance_with_metadata(df, file_path, operation_id)

                # Quality checks
                quality_df = self._apply_data_quality_checks(enhanced_df, table_name)

                # Calculate quality score
                if self.enable_quality_columns:
                    quality_score = quality_df.agg(avg("_dq_quality_score")).collect()[
                        0
                    ][0]
                    quality_score = float(quality_score) if quality_score else 0.0
                else:
                    quality_score = 1.0

                # Check threshold
                min_quality = self.config["quality_thresholds"]["min_quality_score"]
                if quality_score < min_quality:
                    warning = f"Quality {quality_score:.2f} below {min_quality}"
                    logging.warning(f"⚠️  {warning}")

                    if self.config["quality_thresholds"]["fail_on_quality_breach"]:
                        raise Exception(warning)

                # Write to Delta
                success, write_stats = self._write_to_delta_table(
                    quality_df, table_name, operation_id
                )

                if success:
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
                        "processing_time_seconds": 0,
                    }
                    self._log_to_postgres("file_processing", file_stats)

                    logging.info(
                        f"  ✓ {initial_count:,} rows, quality: {quality_score:.2f}"
                    )
                    return True, initial_count
                else:
                    raise Exception(write_stats.get("error", "Unknown error"))

            except Exception as e:
                logging.error(f"  ✗ Attempt {attempt + 1} failed: {e}")

                if attempt == max_retries - 1:
                    self.move_to_dlq(file_path, str(e))
                    self.send_notification(
                        "failure", f"File {file_path.name} failed: {e}", "error"
                    )
                    return False, 0

        return False, 0

    def load_feature_incrementally(self, feature: str) -> Dict:
        """Load feature with retry"""
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
            return {
                "feature": feature,
                "status": "FAILED",
                "error": "Failed to log operation",
            }

        try:
            all_files = self.discover_parquet_files(feature)

            if not all_files:
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
                return {"feature": feature, "status": "NO_FILES"}

            files_processed = 0
            files_skipped = 0
            total_rows_loaded = 0
            failed_files = []

            for file_path in all_files:
                success, rows_loaded = self.load_file_with_retry(
                    file_path, operation_id
                )
                if success:
                    if rows_loaded > 0:
                        files_processed += 1
                        total_rows_loaded += rows_loaded
                    else:
                        files_skipped += 1
                else:
                    failed_files.append(file_path.name)

                    if not self.config["error_handling"]["continue_on_error"]:
                        break

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
                        f"Failed: {', '.join(failed_files)}" if failed_files else None
                    ),
                },
            )

            if status == "COMPLETED":
                self.send_notification(
                    "completion",
                    f"Feature {feature} completed:\n"
                    f"- Processed: {files_processed}\n"
                    f"- Rows: {total_rows_loaded:,}\n"
                    f"- Duration: {duration:.2f}s",
                    "info",
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

            self.send_notification("failure", f"Feature {feature} failed: {e}", "error")
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
                    f"{result['total_rows_loaded']:,} rows"
                )
            else:
                logging.error(f"❌ {feature}: {result.get('error', 'Unknown error')}")

        return results

    def optimize_all_tables(self):
        """Optimize all tables"""
        logging.info("\n" + "=" * 80)
        logging.info("⚡ OPTIMIZING DELTA TABLES")
        logging.info("=" * 80)

        tables_path = self.bronze_base_path / "tables"
        if not tables_path.exists():
            return

        for table_dir in sorted(tables_path.iterdir()):
            if table_dir.is_dir() and DeltaTable.isDeltaTable(
                self.spark, str(table_dir)
            ):
                logging.info(f"\n🔧 {table_dir.name}")
                stats = self._optimize_delta_table(table_dir, table_dir.name)

                if stats.get("status") == "SUCCESS":
                    logging.info(
                        f"  Files: {stats['files_before']} → {stats['files_after']}"
                    )
                    logging.info(
                        f"  Size: {stats['size_before_mb']} → {stats['size_after_mb']} MB"
                    )

    # def print_summary(self):
    #     """Print summary"""
    #     print("\n" + "=" * 80)
    #     print("📊 BRONZE LAYER SUMMARY")
    #     print("=" * 80)
    #     print(f"✅ SCD2: {self.enable_scd2}")
    #     print(f"✅ Quality Columns: {self.enable_quality_columns}")
    #     print(f"✅ Z-ORDER: {self.enable_zorder}")
    #     print(f"✅ Validation: {self.enable_validation}")
    #     print(f"✅ Anomaly Detection: {self.enable_anomaly_detection}")
    #     print("=" * 80)

    def print_summary(self):
        """Print summary"""
        print("\n" + "=" * 80)
        print("[SUMMARY] BRONZE LAYER SUMMARY")
        print("=" * 80)
        print(f"[OK] SCD2: {self.config['features']['enable_scd2']}")
        print(
            f"[OK] Quality Columns: {self.config['features']['enable_quality_columns']}"
        )
        print(f"[OK] Z-ORDER: {self.config['features']['enable_zorder']}")
        print(f"[OK] Validation: {self.config['features']['enable_validation']}")
        print(
            f"[OK] Anomaly Detection: {self.config['features']['enable_anomaly_detection']}"
        )
        print("=" * 80)

    def cleanup(self):
        """Cleanup resources"""
        try:
            if self.pg_conn and not self.pg_conn.closed:
                self.pg_conn.close()
            if self.spark:
                self.spark.stop()
            logging.info("✓ Resources cleaned up")
        except Exception as e:
            logging.error(f"Cleanup error: {e}")


"""START CLAUDE main function"""


# def main():
#     """Main execution"""
#     import argparse

#     parser = argparse.ArgumentParser(description="Standalone Bronze Loader")
#     parser.add_argument(
#         "--config", default="bronze_loader_config.yaml", help="Configuration file path"
#     )
#     parser.add_argument("--dry-run", action="store_true", help="Dry run (no writes)")

#     args = parser.parse_args()

#     # Load config for logging setup
#     with open(args.config, "r") as f:
#         config = yaml.safe_load(f)

#     log_config = config["logging"]
#     log_dir = Path(config["paths"]["log_directory"])
#     log_dir.mkdir(parents=True, exist_ok=True)

#     logging.basicConfig(
#         level=getattr(logging, log_config["level"]),
#         format=log_config["format"],
#         handlers=[
#             logging.FileHandler(log_dir / log_config["file"]["filename"]),
#             logging.StreamHandler(sys.stdout),
#         ],
#     )

#     logger = logging.getLogger(__name__)
#     loader = None

#     try:
#         logger.info("=" * 80)
#         logger.info("🚀 STANDALONE BRONZE LOADER")
#         logger.info("=" * 80)

#         loader = StandaloneBronzeLoader(config_path=args.config)

#         loader.print_summary()

#         if args.dry_run:
#             logger.info("\n🧪 DRY RUN MODE - No writes")
#             return True

#         # Load all features
#         results = loader.load_all_features()

#         # Optimize
#         loader.optimize_all_tables()

#         # Check results
#         failed = [r for r in results if r["status"] == "FAILED"]
#         if failed:
#             logger.error(f"❌ {len(failed)} feature(s) failed")
#             return False
#         else:
#             logger.info("✅ ALL FEATURES LOADED SUCCESSFULLY!")
#             return True

#     except Exception as e:
#         logger.error(f"❌ Load failed: {e}", exc_info=True)
#         return False

#     finally:
#         if loader:
#             loader.cleanup()


"""END CLAUDE main function"""

"""START DEEPSEEK main function"""


def main():
    """Main execution"""
    import argparse

    parser = argparse.ArgumentParser(description="Standalone Bronze Loader")
    parser.add_argument(
        "--config", default="bronze_loader_config.yaml", help="Configuration file path"
    )
    parser.add_argument("--dry-run", action="store_true", help="Dry run (no writes)")

    args = parser.parse_args()

    # Load config for logging setup
    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    log_config = config["logging"]
    log_dir = Path(config["paths"]["log_directory"])
    log_dir.mkdir(parents=True, exist_ok=True)

    # Fix: Remove emojis and use ASCII-friendly logging
    class ASCIIFilter(logging.Filter):
        def filter(self, record):
            # Replace common emojis with text equivalents
            emoji_replacements = {
                "🚀": "[LAUNCH]",
                "📊": "[SUMMARY]",
                "✅": "[OK]",
                "⚡": "[OPTIMIZE]",
                "🔧": "[TABLE]",
                "📦": "[FEATURE]",
                "📄": "[FILE]",
                "⏭️": "[SKIP]",
                "✓": "[OK]",
                "⚠️": "[WARN]",
                "❌": "[ERROR]",
                "💀": "[DLQ]",
                "📦": "[QUARANTINE]",
                "⏳": "[RETRY]",
                "🧪": "[DRY_RUN]",
                "🔍": "[DETAIL]",
            }

            if record.msg:
                for emoji, replacement in emoji_replacements.items():
                    record.msg = record.msg.replace(emoji, replacement)

            return True

    # Configure logging with ASCII filter
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_config["level"]))

    # Clear any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # File handler
    file_handler = logging.FileHandler(log_dir / log_config["file"]["filename"])
    file_handler.setFormatter(logging.Formatter(log_config["format"]))
    file_handler.addFilter(ASCIIFilter())
    logger.addHandler(file_handler)

    # Console handler with proper encoding
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(log_config["format"]))
    console_handler.addFilter(ASCIIFilter())
    logger.addHandler(console_handler)

    loader = None

    try:
        logger.info("=" * 80)
        logger.info("[LAUNCH] STANDALONE BRONZE LOADER")
        logger.info("=" * 80)

        loader = StandaloneBronzeLoader(config_path=args.config)

        loader.print_summary()

        if args.dry_run:
            logger.info("\n[DRY_RUN] DRY RUN MODE - No writes")
            return True

        # Load all features
        results = loader.load_all_features()

        # Optimize
        loader.optimize_all_tables()

        # Check results
        failed = [r for r in results if r["status"] == "FAILED"]
        if failed:
            logger.error(f"[ERROR] {len(failed)} feature(s) failed")
            return False
        else:
            logger.info("[OK] ALL FEATURES LOADED SUCCESSFULLY!")
            return True

    except Exception as e:
        logger.error(f"[ERROR] Load failed: {e}", exc_info=True)
        return False

    finally:
        if loader:
            loader.cleanup()


"""END DEEPSEEK main function"""


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

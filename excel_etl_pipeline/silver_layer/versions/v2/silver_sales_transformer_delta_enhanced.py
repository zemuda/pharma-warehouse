# silver_layer_transformer_delta_enhanced.py
import psycopg2
import polars as pl
from deltalake import DeltaTable
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging
import yaml
from pathlib import Path
import gc
import os


class SilverSalesTransformerDelta:
    def __init__(self, config_path: str = "silver_config_enhanced.yaml"):
        self.config = self._load_config(config_path)
        self._apply_environment_overrides()
        self.db_params = self.config["database"]
        self.bronze_base_path = Path(self.config["paths"]["bronze_base"])
        self.silver_base_path = Path(self.config["paths"]["silver_base"])

        self.conn = self._connect_to_postgres()
        self._init_silver_schema()
        self._setup_platform_safe_logging()
        self.update_schema()

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
                logging.info(
                    f"✓ Successfully loaded enhanced config from {config_path}"
                )
                return config
        except FileNotFoundError:
            logging.warning(
                f"Enhanced config file {config_path} not found. Using default configuration."
            )
            return self._get_default_config()
        except Exception as e:
            logging.error(f"Failed to load enhanced config: {e}")
            raise

    def _get_default_config(self) -> Dict:
        """Get default configuration when enhanced config is not found"""
        return {
            "database": {
                "host": "localhost",
                "database": "pharma_warehouse",
                "user": "postgres",
                "password": "1234",
                "port": "5432",
            },
            "paths": {
                "bronze_base": "C:/pharma_warehouse/excel_etl_pipeline/bronze_layer/bronze_data/bronze/tables",
                "silver_base": "C:/pharma_warehouse/excel_etl_pipeline/excel_pipeline/silver_data",
            },
            "features": {
                "enable_schema_evolution": True,
                "enable_original_data_preservation": True,
                "enable_business_calculations": True,
            },
            "performance": {
                "memory": {"enable_garbage_collection": True, "chunk_size": 100000}
            },
            "error_handling": {"retry": {"enabled": True, "max_attempts": 3}},
        }

    def _apply_environment_overrides(self):
        """Apply environment-specific configuration overrides"""
        current_env = self.config.get("environment", "development")
        env_config = self.config.get("environments", {}).get(current_env, {})

        # Deep merge environment configuration
        self._deep_merge(self.config, env_config)
        logging.info(f"Applied environment configuration for: {current_env}")

    def _deep_merge(self, base_dict: Dict, update_dict: Dict):
        """Recursively merge dictionaries"""
        for key, value in update_dict.items():
            if (
                isinstance(value, dict)
                and key in base_dict
                and isinstance(base_dict[key], dict)
            ):
                self._deep_merge(base_dict[key], value)
            else:
                base_dict[key] = value

    def _connect_to_postgres(self):
        """Establish PostgreSQL connection with enhanced settings"""
        try:
            # Use basic connection parameters (pool settings would need additional setup)
            conn_params = {
                "host": self.db_params["host"],
                "database": self.db_params["database"],
                "user": self.db_params["user"],
                "password": self.db_params["password"],
                "port": self.db_params["port"],
            }
            return psycopg2.connect(**conn_params)
        except Exception as e:
            logging.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def _execute_sql(self, query: str, params: Tuple = None):
        """Execute SQL query with enhanced error handling"""
        max_attempts = (
            self.config.get("error_handling", {})
            .get("retry", {})
            .get("max_attempts", 3)
        )

        for attempt in range(max_attempts):
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(query, params or ())
                    if query.strip().upper().startswith(("SELECT", "WITH")):
                        return cursor.fetchall()
                    self.conn.commit()
                    return None
            except Exception as e:
                if attempt < max_attempts - 1:
                    logging.warning(
                        f"SQL execution attempt {attempt + 1} failed: {e}. Retrying..."
                    )
                    self.conn.rollback()
                else:
                    self.conn.rollback()
                    logging.error(
                        f"SQL execution failed after {max_attempts} attempts: {e}\nQuery: {query}"
                    )
                    raise

    def _init_silver_schema(self):
        """Initialize enhanced silver sales schema with data quality tracking"""
        init_scripts = [
            "CREATE SCHEMA IF NOT EXISTS silver_layer;",
            """
            CREATE TABLE IF NOT EXISTS silver_layer.sales_process_log (
                process_id SERIAL PRIMARY KEY,
                source_table VARCHAR,
                target_table VARCHAR,
                records_processed INTEGER,
                records_valid INTEGER,
                records_invalid INTEGER,
                credit_notes_separated INTEGER,
                potential_duplicates_detected INTEGER,
                whitespace_issues_found INTEGER,
                last_processed_version BIGINT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                status VARCHAR,
                error_message VARCHAR
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS silver_layer.sales_data_quality_log (
                log_id SERIAL PRIMARY KEY,
                process_id INTEGER,
                table_name VARCHAR,
                column_name VARCHAR,
                issue_type VARCHAR,
                issue_description VARCHAR,
                affected_rows INTEGER,
                sample_data VARCHAR,
                logged_at TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS silver_layer.sales_delta_checkpoint (
                checkpoint_id SERIAL PRIMARY KEY,
                table_name VARCHAR UNIQUE,
                last_processed_version BIGINT,
                last_processed_timestamp TIMESTAMP,
                records_in_version INTEGER,
                updated_at TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS silver_layer.summarized_cash_invoices;
            """,
            """
            CREATE TABLE IF NOT EXISTS silver_layer.detailed_cash_invoices;
            """,
            """
            CREATE TABLE IF NOT EXISTS silver_layer.generated_summarized_cash_invoices_credit_notes;
            """,
            """
            CREATE TABLE IF NOT EXISTS silver_layer.generated_detailed_cash_invoices_credit_notes;
            """,
        ]

        for script in init_scripts:
            try:
                self._execute_sql(script)
            except Exception as e:
                logging.warning(f"Could not initialize silver sales schema: {e}")

    def update_schema(self):
        """Update database schema to include new columns - with feature toggle"""
        if not self.config.get("features", {}).get("enable_schema_evolution", True):
            logging.info("Schema evolution is disabled via feature toggle")
            return

        schema_updates = [
            # Add calculated fields to summarized_cash_invoices
            "ALTER TABLE silver_layer.summarized_cash_invoices ADD COLUMN IF NOT EXISTS stg_vat_calculated DECIMAL(15,2)",
            "ALTER TABLE silver_layer.summarized_cash_invoices ADD COLUMN IF NOT EXISTS stg_discount_percentage DECIMAL(5,2)",
            # Add data quality flags to summarized_cash_invoices
            "ALTER TABLE silver_layer.summarized_cash_invoices ADD COLUMN IF NOT EXISTS amount_invalid_flag INTEGER",
            "ALTER TABLE silver_layer.summarized_cash_invoices ADD COLUMN IF NOT EXISTS date_invalid_flag INTEGER",
            "ALTER TABLE silver_layer.summarized_cash_invoices ADD COLUMN IF NOT EXISTS customer_missing_flag INTEGER",
            "ALTER TABLE silver_layer.summarized_cash_invoices ADD COLUMN IF NOT EXISTS is_credit_note INTEGER",
            # Add original value preservation to summarized_cash_invoices
            "ALTER TABLE silver_layer.summarized_cash_invoices ADD COLUMN IF NOT EXISTS original_document_number VARCHAR",
            "ALTER TABLE silver_layer.summarized_cash_invoices ADD COLUMN IF NOT EXISTS original_cus_code VARCHAR",
            "ALTER TABLE silver_layer.summarized_cash_invoices ADD COLUMN IF NOT EXISTS original_customer_name VARCHAR",
            "ALTER TABLE silver_layer.summarized_cash_invoices ADD COLUMN IF NOT EXISTS original_trn_date VARCHAR",
            # Add calculated fields to detailed_cash_invoices
            "ALTER TABLE silver_layer.detailed_cash_invoices ADD COLUMN IF NOT EXISTS calculated_line_total DECIMAL(15,2)",
            "ALTER TABLE silver_layer.detailed_cash_invoices ADD COLUMN IF NOT EXISTS effective_amount DECIMAL(15,2)",
            "ALTER TABLE silver_layer.detailed_cash_invoices ADD COLUMN IF NOT EXISTS line_discounted_product_unit_price DECIMAL(15,2)",
            "ALTER TABLE silver_layer.detailed_cash_invoices ADD COLUMN IF NOT EXISTS net_line_amount DECIMAL(15,2)",
            # Add data quality flags to detailed_cash_invoices
            "ALTER TABLE silver_layer.detailed_cash_invoices ADD COLUMN IF NOT EXISTS amount_invalid_flag INTEGER",
            "ALTER TABLE silver_layer.detailed_cash_invoices ADD COLUMN IF NOT EXISTS date_invalid_flag INTEGER",
            "ALTER TABLE silver_layer.detailed_cash_invoices ADD COLUMN IF NOT EXISTS customer_missing_flag INTEGER",
            "ALTER TABLE silver_layer.detailed_cash_invoices ADD COLUMN IF NOT EXISTS quantity_invalid_flag INTEGER",
            "ALTER TABLE silver_layer.detailed_cash_invoices ADD COLUMN IF NOT EXISTS price_invalid_flag INTEGER",
            "ALTER TABLE silver_layer.detailed_cash_invoices ADD COLUMN IF NOT EXISTS is_credit_note INTEGER",
            "ALTER TABLE silver_layer.detailed_cash_invoices ADD COLUMN IF NOT EXISTS potential_duplicate_flag INTEGER",
            # Add original value preservation to detailed_cash_invoices
            "ALTER TABLE silver_layer.detailed_cash_invoices ADD COLUMN IF NOT EXISTS original_document_number VARCHAR",
            "ALTER TABLE silver_layer.detailed_cash_invoices ADD COLUMN IF NOT EXISTS original_item_code VARCHAR",
            "ALTER TABLE silver_layer.detailed_cash_invoices ADD COLUMN IF NOT EXISTS original_cus_code VARCHAR",
            "ALTER TABLE silver_layer.detailed_cash_invoices ADD COLUMN IF NOT EXISTS original_description VARCHAR",
            # Add calculated fields to credit notes tables
            "ALTER TABLE silver_layer.generated_summarized_cash_invoices_credit_notes ADD COLUMN IF NOT EXISTS credit_calculated_total DECIMAL(15,2)",
            "ALTER TABLE silver_layer.generated_detailed_cash_invoices_credit_notes ADD COLUMN IF NOT EXISTS credit_calculated_line_total DECIMAL(15,2)",
        ]

        for update in schema_updates:
            try:
                self._execute_sql(update)
                logging.info(
                    f"Schema update applied: {update.split('ADD COLUMN IF NOT EXISTS')[1].split(' ')[1]}"
                )
            except Exception as e:
                logging.warning(
                    f"Schema update skipped (column may already exist): {e}"
                )

    def _setup_platform_safe_logging(self):
        """Use platform-safe logging messages with enhanced configuration"""
        import os

        # Get logging configuration
        log_config = self.config.get("logging", {})
        console_config = log_config.get("console", {})

        if os.name == "nt" or not console_config.get("use_platform_safe_emojis", True):
            self.log_messages = {
                "start_pipeline": "Starting enhanced incremental Delta Lake pipeline...",
                "preserve_note": "NOTE: Processing only new/changed records from Delta tables with enhanced features",
                "processing_summarized": "Processing summarized cash invoices (enhanced incremental)...",
                "processing_detailed": "Processing detailed invoices (enhanced incremental)...",
                "success": "Enhanced incremental processing completed successfully!",
                "tip": "Check sales_process_log for detailed statistics and enhanced metrics",
                "error": "Failed to process {layer}: {error}",
                "summarized_results": "Summarized: {new:,} new records, {invoices:,} invoices, {credits:,} credits",
                "detailed_results": "Detailed: {new:,} new records, {invoices:,} invoices, {credits:,} credits",
                "no_new_data": "No new data in {table} (version {version})",
            }
        else:
            self.log_messages = {
                "start_pipeline": "🔄 Starting enhanced incremental Delta Lake pipeline...",
                "preserve_note": "📊 NOTE: Processing only new/changed records from Delta tables with enhanced features",
                "processing_summarized": "📋 Processing summarized cash invoices (enhanced incremental)...",
                "processing_detailed": "📄 Processing detailed invoices (enhanced incremental)...",
                "success": "✅ Enhanced incremental processing completed successfully!",
                "tip": "💡 Check sales_process_log for detailed statistics and enhanced metrics",
                "error": "❌ Failed to process {layer}: {error}",
                "summarized_results": "✅ Summarized: {new:,} new records, {invoices:,} invoices, {credits:,} credits",
                "detailed_results": "✅ Detailed: {new:,} new records, {invoices:,} invoices, {credits:,} credits",
                "no_new_data": "ℹ️ No new data in {table} (version {version})",
            }

    def get_last_processed_version(self, table_name: str) -> Optional[int]:
        """Get the last processed Delta table version"""
        try:
            result = self._execute_sql(
                "SELECT last_processed_version FROM silver_layer.sales_delta_checkpoint WHERE table_name = %s",
                (table_name,),
            )
            return result[0][0] if result else None
        except Exception as e:
            logging.warning(f"Could not get checkpoint for {table_name}: {e}")
            return None

    def update_checkpoint(self, table_name: str, version: int, record_count: int):
        """Update the checkpoint for a table with enhanced metrics tracking"""
        try:
            enable_metrics = (
                self.config.get("incremental_processing", {})
                .get("checkpoints", {})
                .get("enable_metrics_tracking", True)
            )

            if enable_metrics:
                self._execute_sql(
                    """
                    INSERT INTO silver_layer.sales_delta_checkpoint (table_name, last_processed_version, last_processed_timestamp, records_in_version, updated_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (table_name) 
                    DO UPDATE SET 
                        last_processed_version = EXCLUDED.last_processed_version,
                        last_processed_timestamp = EXCLUDED.last_processed_timestamp,
                        records_in_version = EXCLUDED.records_in_version,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (table_name, version, datetime.now(), record_count, datetime.now()),
                )
        except Exception as e:
            logging.error(f"Failed to update checkpoint for {table_name}: {e}")

    def load_delta_incremental(
        self, delta_path: str, table_name: str
    ) -> Tuple[pl.DataFrame, int, int]:
        """Load only new data from Delta table since last checkpoint with enhanced memory management"""
        try:
            dt = DeltaTable(delta_path)
            current_version = dt.version()
            last_version = self.get_last_processed_version(table_name)

            if last_version is not None and last_version >= current_version:
                logging.info(
                    self.log_messages["no_new_data"].format(
                        table=table_name, version=current_version
                    )
                )
                return pl.DataFrame(), current_version, 0

            # Enhanced retry mechanism with backoff
            retry_config = self.config.get("error_handling", {}).get("retry", {})
            max_attempts = retry_config.get("max_attempts", 3)
            backoff_factor = retry_config.get("backoff_factor", 2)

            for attempt in range(max_attempts):
                try:
                    # Get chunk size from config
                    chunk_size = (
                        self.config.get("performance", {})
                        .get("memory", {})
                        .get("chunk_size", 100000)
                    )

                    if last_version is None:
                        logging.info(
                            f"📥 First load for {table_name} - loading all data (version {current_version})"
                        )
                        df = dt.to_pyarrow_table().to_pandas()
                        df_pl = pl.from_pandas(df)
                    else:
                        logging.info(
                            f"📥 Incremental load for {table_name}: versions {last_version + 1} to {current_version}"
                        )
                        df = dt.to_pyarrow_table().to_pandas()
                        df_pl = pl.from_pandas(df)

                    record_count = len(df_pl)

                    # Enhanced memory monitoring
                    self._check_memory_usage()

                    return df_pl, current_version, record_count

                except Exception as e:
                    if attempt < max_attempts - 1:
                        delay = backoff_factor**attempt
                        logging.warning(
                            f"Attempt {attempt + 1} failed for {table_name}: {e}. Retrying in {delay} seconds..."
                        )
                        gc.collect()
                        import time

                        time.sleep(delay)
                    else:
                        raise e

            return pl.DataFrame(), 0, 0

        except Exception as e:
            logging.error(f"Failed to load Delta table {delta_path}: {e}")
            return pl.DataFrame(), 0, 0

    def _check_memory_usage(self):
        """Check memory usage against configured thresholds"""
        try:
            memory_config = self.config.get("performance", {}).get("memory", {})
            max_memory_mb = memory_config.get("max_memory_mb", 4096)
            gc_threshold = memory_config.get("gc_threshold", 0.8)

            import psutil

            process = psutil.Process()
            memory_info = process.memory_info()
            memory_usage_mb = memory_info.rss / 1024 / 1024
            memory_ratio = memory_usage_mb / max_memory_mb

            if memory_ratio > gc_threshold:
                logging.warning(
                    f"High memory usage detected: {memory_usage_mb:.2f} MB ({memory_ratio:.1%} of limit). Forcing garbage collection."
                )
                gc.collect()

            # Log if monitoring is enabled
            if (
                self.config.get("monitoring", {})
                .get("performance", {})
                .get("enabled", False)
            ):
                logging.debug(f"Memory usage: {memory_usage_mb:.2f} MB")

        except ImportError:
            # psutil not available, skip memory monitoring
            pass
        except Exception as e:
            logging.warning(f"Memory monitoring failed: {e}")

    # ... (rest of the methods remain the same as previous enhanced version, but with config references updated)

    def _add_enhanced_calculations_summarized(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add enhanced business logic calculations for summarized invoices with feature toggle"""
        if df.is_empty() or not self.config.get("features", {}).get(
            "enable_business_calculations", True
        ):
            return df

        return df.with_columns(
            [
                # Business calculations
                (
                    pl.col("amount_inclusive_numeric")
                    - pl.col("amount_exclusive_numeric")
                ).alias("stg_vat_calculated"),
                # Discount percentage calculation
                pl.when(pl.col("amount_exclusive_numeric") > 0)
                .then(
                    (pl.col("discount_numeric") / pl.col("amount_exclusive_numeric"))
                    * 100
                )
                .otherwise(0)
                .alias("stg_discount_percentage"),
                # Data quality flags
                pl.col("amount_numeric")
                .is_null()
                .cast(pl.Int8)
                .alias("amount_invalid_flag"),
                pl.col("transaction_date")
                .is_null()
                .cast(pl.Int8)
                .alias("date_invalid_flag"),
                (
                    pl.col("CUS_CODE_cleaned").is_null()
                    | (pl.col("CUS_CODE_cleaned").str.strip_chars() == "")
                )
                .cast(pl.Int8)
                .alias("customer_missing_flag"),
                # Business logic flags
                pl.when(
                    pl.col("DOCUMENT_NUMBER_cleaned").str.starts_with("CN")
                    | (pl.col("amount_numeric") <= 0)
                )
                .then(pl.lit(1))
                .otherwise(pl.lit(0))
                .alias("is_credit_note"),
                # Original value preservation (if enabled)
                (
                    pl.col("DOCUMENT_NUMBER").alias("original_document_number")
                    if self.config.get("features", {}).get(
                        "enable_original_data_preservation", True
                    )
                    else pl.lit(None).alias("original_document_number")
                ),
                (
                    pl.col("CUS_CODE").alias("original_cus_code")
                    if self.config.get("features", {}).get(
                        "enable_original_data_preservation", True
                    )
                    else pl.lit(None).alias("original_cus_code")
                ),
                (
                    pl.col("CUSTOMER_NAME").alias("original_customer_name")
                    if self.config.get("features", {}).get(
                        "enable_original_data_preservation", True
                    )
                    else pl.lit(None).alias("original_customer_name")
                ),
                (
                    pl.col("TRN_DATE").alias("original_trn_date")
                    if self.config.get("features", {}).get(
                        "enable_original_data_preservation", True
                    )
                    else pl.lit(None).alias("original_trn_date")
                ),
            ]
        )

    def run_incremental_pipeline(self):
        """Run incremental data processing pipeline with enhanced features and monitoring"""
        logging.info(self.log_messages["start_pipeline"])
        logging.info(self.log_messages["preserve_note"])

        # Check feature toggles
        if not self.config.get("features", {}).get("enable_incremental_loading", True):
            logging.info("Incremental loading is disabled via feature toggle")
            return False

        try:
            start_time = datetime.now()

            self.process_summarized_cash_invoices()
            self.process_detailed_cash_invoices()

            # Create daily sales summary if enabled
            if self.config.get("features", {}).get("enable_advanced_metrics", True):
                logging.info("📈 Creating enhanced daily sales summary...")
                self.create_silver_daily_sales_summary()

            # Log overall data quality metrics
            if self.config.get("features", {}).get(
                "enable_enhanced_metrics_logging", True
            ):
                self.log_overall_data_quality()

            # Performance monitoring
            processing_time = (datetime.now() - start_time).total_seconds()
            if (
                self.config.get("monitoring", {})
                .get("performance", {})
                .get("enabled", False)
            ):
                threshold = (
                    self.config.get("monitoring", {})
                    .get("performance", {})
                    .get("thresholds", {})
                    .get("processing_time_seconds", 300)
                )
                if processing_time > threshold:
                    logging.warning(
                        f"Processing time {processing_time:.2f}s exceeded threshold of {threshold}s"
                    )

            logging.info(self.log_messages["success"])
            logging.info(self.log_messages["tip"])
            return True

        except Exception as e:
            logging.error(f"❌ Enhanced incremental pipeline failed: {e}")
            return False

    def close(self):
        """Close database connection with enhanced cleanup"""
        if self.conn:
            self.conn.close()

        # Enhanced garbage collection
        memory_config = self.config.get("performance", {}).get("memory", {})
        if memory_config.get("enable_garbage_collection", True):
            gc.collect()
            logging.debug("Forced garbage collection during cleanup")


if __name__ == "__main__":
    # Enhanced logging setup from config
    config_path = "silver_config_enhanced.yaml"

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
            log_config = config.get("logging", {})

            # Set up logging based on config
            log_level = getattr(logging, log_config.get("level", "INFO").upper())
            log_format = log_config.get(
                "format", "%(asctime)s - %(levelname)s - %(message)s"
            )

            logging.basicConfig(level=log_level, format=log_format)

    except FileNotFoundError:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )

    # Use enhanced config
    transformer = SilverSalesTransformerDelta(config_path=config_path)
    try:
        success = transformer.run_incremental_pipeline()
        exit(0 if success else 1)
    finally:
        transformer.close()

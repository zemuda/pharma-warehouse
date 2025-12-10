# silver_sales_transformer_delta_enhanced.py
import psycopg2
import polars as pl
from deltalake import DeltaTable
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging
import yaml
from pathlib import Path
import gc


class SilverSalesTransformerDelta:
    def __init__(self, config_path: str = "silver_config.yaml"):
        self.config = self._load_config(config_path)
        self.db_params = self.config["database"]
        self.bronze_base_path = Path(self.config["paths"]["bronze_base"])
        self.silver_base_path = Path(self.config["paths"]["silver_base"])

        self.conn = self._connect_to_postgres()
        self._init_silver_schema()
        self._setup_platform_safe_logging()
        self.update_schema()  # Ensure schema is up to date

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
                logging.info(f"✓ Successfully loaded config from {config_path}")
                return config
        except FileNotFoundError:
            logging.warning(
                f"Config file {config_path} not found. Using default configuration."
            )
            # Fallback to default configuration
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
            }
        except Exception as e:
            logging.error(f"Failed to load config: {e}")
            raise

    def _connect_to_postgres(self):
        """Establish PostgreSQL connection"""
        try:
            return psycopg2.connect(**self.db_params)
        except Exception as e:
            logging.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def _execute_sql(self, query: str, params: Tuple = None):
        """Execute SQL query with error handling"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params or ())
                if query.strip().upper().startswith(("SELECT", "WITH")):
                    return cursor.fetchall()
                self.conn.commit()
                return None
        except Exception as e:
            self.conn.rollback()
            logging.error(f"SQL execution failed: {e}\nQuery: {query}")
            raise

    def _init_silver_schema(self):
        """Initialize enhanced silver sales schema with data quality tracking"""
        init_scripts = [
            "CREATE SCHEMA IF NOT EXISTS silver_sales;",
            """
            CREATE TABLE IF NOT EXISTS silver_sales.process_log (
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
            CREATE TABLE IF NOT EXISTS silver_sales.data_quality_log (
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
            CREATE TABLE IF NOT EXISTS silver_sales.delta_checkpoint (
                checkpoint_id SERIAL PRIMARY KEY,
                table_name VARCHAR UNIQUE,
                last_processed_version BIGINT,
                last_processed_timestamp TIMESTAMP,
                records_in_version INTEGER,
                updated_at TIMESTAMP
            );
            """,
        ]

        for script in init_scripts:
            try:
                self._execute_sql(script)
            except Exception as e:
                logging.warning(f"Could not initialize silver sales schema: {e}")

    def update_schema(self):
        """Update database schema to include new columns - matches transactional version features"""
        schema_updates = [
            # Add calculated fields to summarized_cash_invoices
            "ALTER TABLE silver_sales.summarized_cash_invoices ADD COLUMN IF NOT EXISTS stg_vat_calculated DECIMAL(15,2)",
            "ALTER TABLE silver_sales.summarized_cash_invoices ADD COLUMN IF NOT EXISTS stg_discount_percentage DECIMAL(5,2)",
            # Add data quality flags to summarized_cash_invoices
            "ALTER TABLE silver_sales.summarized_cash_invoices ADD COLUMN IF NOT EXISTS amount_invalid_flag INTEGER",
            "ALTER TABLE silver_sales.summarized_cash_invoices ADD COLUMN IF NOT EXISTS date_invalid_flag INTEGER",
            "ALTER TABLE silver_sales.summarized_cash_invoices ADD COLUMN IF NOT EXISTS customer_missing_flag INTEGER",
            "ALTER TABLE silver_sales.summarized_cash_invoices ADD COLUMN IF NOT EXISTS is_credit_note INTEGER",
            # Add original value preservation to summarized_cash_invoices
            "ALTER TABLE silver_sales.summarized_cash_invoices ADD COLUMN IF NOT EXISTS original_document_number VARCHAR",
            "ALTER TABLE silver_sales.summarized_cash_invoices ADD COLUMN IF NOT EXISTS original_cus_code VARCHAR",
            "ALTER TABLE silver_sales.summarized_cash_invoices ADD COLUMN IF NOT EXISTS original_customer_name VARCHAR",
            "ALTER TABLE silver_sales.summarized_cash_invoices ADD COLUMN IF NOT EXISTS original_trn_date VARCHAR",
            # Add calculated fields to detailed_cash_invoices
            "ALTER TABLE silver_sales.detailed_cash_invoices ADD COLUMN IF NOT EXISTS calculated_line_total DECIMAL(15,2)",
            "ALTER TABLE silver_sales.detailed_cash_invoices ADD COLUMN IF NOT EXISTS effective_amount DECIMAL(15,2)",
            "ALTER TABLE silver_sales.detailed_cash_invoices ADD COLUMN IF NOT EXISTS line_discounted_product_unit_price DECIMAL(15,2)",
            "ALTER TABLE silver_sales.detailed_cash_invoices ADD COLUMN IF NOT EXISTS net_line_amount DECIMAL(15,2)",
            # Add data quality flags to detailed_cash_invoices
            "ALTER TABLE silver_sales.detailed_cash_invoices ADD COLUMN IF NOT EXISTS amount_invalid_flag INTEGER",
            "ALTER TABLE silver_sales.detailed_cash_invoices ADD COLUMN IF NOT EXISTS date_invalid_flag INTEGER",
            "ALTER TABLE silver_sales.detailed_cash_invoices ADD COLUMN IF NOT EXISTS customer_missing_flag INTEGER",
            "ALTER TABLE silver_sales.detailed_cash_invoices ADD COLUMN IF NOT EXISTS quantity_invalid_flag INTEGER",
            "ALTER TABLE silver_sales.detailed_cash_invoices ADD COLUMN IF NOT EXISTS price_invalid_flag INTEGER",
            "ALTER TABLE silver_sales.detailed_cash_invoices ADD COLUMN IF NOT EXISTS is_credit_note INTEGER",
            "ALTER TABLE silver_sales.detailed_cash_invoices ADD COLUMN IF NOT EXISTS potential_duplicate_flag INTEGER",
            # Add original value preservation to detailed_cash_invoices
            "ALTER TABLE silver_sales.detailed_cash_invoices ADD COLUMN IF NOT EXISTS original_document_number VARCHAR",
            "ALTER TABLE silver_sales.detailed_cash_invoices ADD COLUMN IF NOT EXISTS original_item_code VARCHAR",
            "ALTER TABLE silver_sales.detailed_cash_invoices ADD COLUMN IF NOT EXISTS original_cus_code VARCHAR",
            "ALTER TABLE silver_sales.detailed_cash_invoices ADD COLUMN IF NOT EXISTS original_description VARCHAR",
            # Add calculated fields to credit notes tables
            "ALTER TABLE silver_sales.generated_summarized_credit_notes ADD COLUMN IF NOT EXISTS credit_calculated_total DECIMAL(15,2)",
            "ALTER TABLE silver_sales.generated_detailed_credit_notes ADD COLUMN IF NOT EXISTS credit_calculated_line_total DECIMAL(15,2)",
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
        """Use platform-safe logging messages"""
        import os

        if os.name == "nt":
            self.log_messages = {
                "start_pipeline": "Starting incremental Delta Lake pipeline...",
                "preserve_note": "NOTE: Processing only new/changed records from Delta tables",
                "processing_summarized": "Processing summarized cash invoices (incremental)...",
                "processing_detailed": "Processing detailed invoices (incremental)...",
                "success": "Incremental processing completed successfully!",
                "tip": "Check process_log for detailed statistics",
                "error": "Failed to process {layer}: {error}",
                "summarized_results": "Summarized: {new:,} new records, {invoices:,} invoices, {credits:,} credits",
                "detailed_results": "Detailed: {new:,} new records, {invoices:,} invoices, {credits:,} credits",
                "no_new_data": "No new data in {table} (version {version})",
            }
        else:
            self.log_messages = {
                "start_pipeline": "🔄 Starting incremental Delta Lake pipeline...",
                "preserve_note": "📊 NOTE: Processing only new/changed records from Delta tables",
                "processing_summarized": "📋 Processing summarized cash invoices (incremental)...",
                "processing_detailed": "📄 Processing detailed invoices (incremental)...",
                "success": "✅ Incremental processing completed successfully!",
                "tip": "💡 Check process_log for detailed statistics",
                "error": "❌ Failed to process {layer}: {error}",
                "summarized_results": "✅ Summarized: {new:,} new records, {invoices:,} invoices, {credits:,} credits",
                "detailed_results": "✅ Detailed: {new:,} new records, {invoices:,} invoices, {credits:,} credits",
                "no_new_data": "ℹ️ No new data in {table} (version {version})",
            }

    def get_last_processed_version(self, table_name: str) -> Optional[int]:
        """Get the last processed Delta table version"""
        try:
            result = self._execute_sql(
                "SELECT last_processed_version FROM silver_sales.delta_checkpoint WHERE table_name = %s",
                (table_name,),
            )
            return result[0][0] if result else None
        except Exception as e:
            logging.warning(f"Could not get checkpoint for {table_name}: {e}")
            return None

    def update_checkpoint(self, table_name: str, version: int, record_count: int):
        """Update the checkpoint for a table"""
        try:
            self._execute_sql(
                """
                INSERT INTO silver_sales.delta_checkpoint (table_name, last_processed_version, last_processed_timestamp, records_in_version, updated_at)
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
        """Load only new data from Delta table since last checkpoint with memory management"""
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

            # Load data with retry mechanism
            max_attempts = (
                self.config.get("error_handling", {})
                .get("retry", {})
                .get("max_attempts", 3)
            )
            for attempt in range(max_attempts):
                try:
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
                    return df_pl, current_version, record_count

                except Exception as e:
                    if attempt < max_attempts - 1:
                        logging.warning(
                            f"Attempt {attempt + 1} failed for {table_name}: {e}. Retrying..."
                        )
                        gc.collect()  # Force garbage collection
                    else:
                        raise e

            return pl.DataFrame(), 0, 0

        except Exception as e:
            logging.error(f"Failed to load Delta table {delta_path}: {e}")
            return pl.DataFrame(), 0, 0

    def log_data_quality_issue(
        self,
        process_id: int,
        table_name: str,
        column_name: str,
        issue_type: str,
        description: str,
        affected_rows: int,
        sample_data: str = "",
    ):
        """Log data quality issues for monitoring"""
        try:
            self._execute_sql(
                """
                INSERT INTO silver_sales.data_quality_log 
                (process_id, table_name, column_name, issue_type, issue_description, affected_rows, sample_data, logged_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    process_id,
                    table_name,
                    column_name,
                    issue_type,
                    description,
                    affected_rows,
                    sample_data,
                    datetime.now(),
                ),
            )
        except Exception as e:
            logging.error(f"Failed to log data quality issue: {e}")

    def detect_and_log_whitespace(
        self, df: pl.DataFrame, table_name: str, process_id: int
    ) -> int:
        """Detect and log columns with trailing whitespace"""
        whitespace_count = 0
        try:
            for col in df.columns:
                if df[col].dtype == pl.Utf8:
                    ws_rows = df.filter(
                        pl.col(col).str.strip_chars() != pl.col(col)
                    ).height
                    if ws_rows > 0:
                        whitespace_count += 1
                        sample = (
                            df.filter(pl.col(col).str.strip_chars() != pl.col(col))
                            .select(pl.col(col).head(3))
                            .to_series()
                            .to_list()
                        )
                        self.log_data_quality_issue(
                            process_id,
                            table_name,
                            col,
                            "TRAILING_WHITESPACE",
                            f"Found {ws_rows} rows with whitespace",
                            ws_rows,
                            str(sample),
                        )
        except Exception as e:
            logging.warning(f"Could not check whitespace: {e}")
        return whitespace_count

    def detect_potential_duplicates(
        self, df: pl.DataFrame, key_columns: List[str]
    ) -> int:
        """Detect potential duplicate transactions"""
        try:
            if df.is_empty():
                return 0
            duplicates = (
                df.group_by(key_columns)
                .agg(pl.count().alias("cnt"))
                .filter(pl.col("cnt") > 1)
                .height
            )
            return duplicates
        except Exception as e:
            logging.error(f"Failed to detect duplicates: {e}")
            return 0

    def _clean_data_with_polars(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean and transform data using Polars - preserves raw data and adds enhanced features"""
        if df.is_empty():
            return df

        # Clean string columns - CREATE NEW COLUMNS to preserve raw data
        str_cols = [col for col in df.columns if df[col].dtype == pl.Utf8]

        df_cleaned = df.with_columns(
            [pl.col(col).str.strip_chars().name.suffix("_cleaned") for col in str_cols]
        )

        # Handle numeric conversions
        numeric_mappings = {
            "AMOUNT": "amount_numeric",
            "AMOUNT_INCLUSIVE": "amount_inclusive_numeric",
            "AMOUNT_EXCLUSIVE": "amount_exclusive_numeric",
            "VAT": "vat_numeric",
            "DISC": "discount_numeric",
        }

        for old_col, new_col in numeric_mappings.items():
            if old_col in df_cleaned.columns:
                df_cleaned = df_cleaned.with_columns(
                    pl.col(old_col).cast(pl.Float64, strict=False).alias(new_col)
                )

        # Parse dates
        if "TRN_DATE" in df_cleaned.columns:
            df_cleaned = df_cleaned.with_columns(
                [
                    pl.col("TRN_DATE")
                    .str.strptime(pl.Date, strict=False)
                    .alias("transaction_date"),
                    pl.col("TRN_DATE")
                    .str.strptime(pl.Datetime, strict=False)
                    .dt.year()
                    .alias("transaction_year"),
                    pl.col("TRN_DATE")
                    .str.strptime(pl.Datetime, strict=False)
                    .dt.month()
                    .alias("transaction_month"),
                    pl.col("TRN_DATE")
                    .str.strptime(pl.Datetime, strict=False)
                    .dt.time()
                    .alias("transaction_time"),
                ]
            )

        return df_cleaned

    def _add_enhanced_calculations_summarized(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add enhanced business logic calculations for summarized invoices"""
        if df.is_empty():
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
                # Original value preservation
                pl.col("DOCUMENT_NUMBER").alias("original_document_number"),
                pl.col("CUS_CODE").alias("original_cus_code"),
                pl.col("CUSTOMER_NAME").alias("original_customer_name"),
                pl.col("TRN_DATE").alias("original_trn_date"),
            ]
        )

    def _add_enhanced_calculations_detailed(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add enhanced business logic calculations for detailed invoices"""
        if df.is_empty():
            return df

        # First add basic calculations
        df_calculated = df.with_columns(
            [
                # Line item calculations
                (
                    pl.col("QUANTITY").cast(pl.Float64, strict=False)
                    * pl.col("ITEM_PRICE").cast(pl.Float64, strict=False)
                ).alias("calculated_line_total"),
                # Effective amount (prioritize calculated over raw amount)
                pl.when(
                    (pl.col("QUANTITY").cast(pl.Float64, strict=False) > 0)
                    & (pl.col("ITEM_PRICE").cast(pl.Float64, strict=False) > 0)
                )
                .then(
                    pl.col("QUANTITY").cast(pl.Float64, strict=False)
                    * pl.col("ITEM_PRICE").cast(pl.Float64, strict=False)
                )
                .otherwise(pl.col("amount_numeric"))
                .alias("effective_amount"),
                # Discounted unit price
                (
                    pl.col("ITEM_PRICE").cast(pl.Float64, strict=False)
                    * (
                        1
                        - pl.col("DISCOUNT_PERCENT")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0)
                        / 100
                    )
                ).alias("line_discounted_product_unit_price"),
                # Net line amount
                (
                    pl.col("QUANTITY").cast(pl.Float64, strict=False)
                    * pl.col("ITEM_PRICE").cast(pl.Float64, strict=False)
                    * (
                        1
                        - pl.col("DISCOUNT_PERCENT")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0)
                        / 100
                    )
                ).alias("net_line_amount"),
            ]
        )

        # Then add flags and original values
        return df_calculated.with_columns(
            [
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
                (
                    pl.col("QUANTITY").is_null()
                    | (pl.col("QUANTITY").cast(pl.Float64, strict=False) <= 0)
                )
                .cast(pl.Int8)
                .alias("quantity_invalid_flag"),
                (
                    pl.col("ITEM_PRICE").is_null()
                    | (pl.col("ITEM_PRICE").cast(pl.Float64, strict=False) <= 0)
                )
                .cast(pl.Int8)
                .alias("price_invalid_flag"),
                # Business logic flags
                pl.when(
                    pl.col("DOCUMENT_NUMBER_cleaned").str.starts_with("CN")
                    | (pl.col("amount_numeric") <= 0)
                )
                .then(pl.lit(1))
                .otherwise(pl.lit(0))
                .alias("is_credit_note"),
                # Original value preservation
                pl.col("DOCUMENT_NUMBER").alias("original_document_number"),
                pl.col("ITEM_CODE").alias("original_item_code"),
                pl.col("CUS_CODE").alias("original_cus_code"),
                pl.col("DESCRIPTION").alias("original_description"),
            ]
        )

    def _add_per_record_duplicate_flags(
        self, df: pl.DataFrame, key_columns: List[str]
    ) -> pl.DataFrame:
        """Add per-record duplicate detection flags"""
        if df.is_empty():
            return df

        # Add window function for duplicate detection
        window_expr = pl.struct(key_columns)
        return df.with_columns(
            [
                (pl.col("cash_invoice_number").count().over(window_expr) > 1)
                .cast(pl.Int8)
                .alias("potential_duplicate_flag")
            ]
        )

    def write_to_postgres(
        self, df: pl.DataFrame, table_name: str, if_exists: str = "append"
    ):
        """Write DataFrame to PostgreSQL using psycopg2 connection with memory management"""
        try:
            # Enable garbage collection if configured
            if (
                self.config.get("performance", {})
                .get("memory", {})
                .get("enable_garbage_collection", True)
            ):
                gc.collect()

            # Create SQLAlchemy connection URI from psycopg2 config
            uri = f"postgresql://{self.db_params['user']}:{self.db_params['password']}@{self.db_params['host']}:{self.db_params['port']}/{self.db_params['database']}"

            df.write_database(table_name, connection=uri, if_table_exists=if_exists)
        except Exception as e:
            logging.error(f"Failed to write to {table_name}: {e}")
            raise

    def process_summarized_cash_invoices(self):
        """Process summarized cash invoices incrementally with enhanced features"""
        table_name = "bronze_sales_cash_invoices_summarized"
        delta_path = str(self.bronze_base_path / table_name)

        process_id = self._execute_sql(
            "INSERT INTO silver_sales.process_log (source_table, target_table, started_at, status) VALUES (%s, %s, %s, %s) RETURNING process_id",
            (
                table_name,
                "silver_sales.summarized_cash_invoices",
                datetime.now(),
                "STARTED",
            ),
        )[0][0]

        try:
            # Load incremental data
            df_new, current_version, record_count = self.load_delta_incremental(
                delta_path, table_name
            )

            if df_new.is_empty():
                self._execute_sql(
                    "UPDATE silver_sales.process_log SET completed_at = %s, status = 'NO_NEW_DATA', last_processed_version = %s WHERE process_id = %s",
                    (datetime.now(), current_version, process_id),
                )
                return True

            # Data quality checks
            whitespace_issues = self.detect_and_log_whitespace(
                df_new, table_name, process_id
            )
            potential_duplicates = self.detect_potential_duplicates(
                df_new, ["DOCUMENT_NUMBER", "CUS_CODE", "TRN_DATE", "AMOUNT"]
            )

            if potential_duplicates > 0:
                self.log_data_quality_issue(
                    process_id,
                    table_name,
                    "ALL",
                    "POTENTIAL_DUPLICATES",
                    f"Found {potential_duplicates} potential duplicates",
                    potential_duplicates,
                    "",
                )

            # Clean data
            df_cleaned = self._clean_data_with_polars(df_new)

            # Separate invoices and credits
            df_invoices = df_cleaned.filter(
                (pl.col("DOCUMENT_NUMBER").is_not_null())
                & (pl.col("amount_numeric") > 0)
            )

            df_credits = df_cleaned.filter(
                (pl.col("DOCUMENT_NUMBER").is_not_null())
                & (pl.col("amount_numeric") <= 0)
            )

            # Transform and write invoices with enhanced features
            if not df_invoices.is_empty():
                # Add enhanced calculations
                df_invoices_enhanced = self._add_enhanced_calculations_summarized(
                    df_invoices
                )

                # Get additional columns if they exist
                optional_cols = []
                for col in [
                    "BRANCH_NAME",
                    "BRANCH_CODE",
                    "SALES_USERNAME",
                    "USER_CODE",
                ]:
                    col_cleaned = col + "_cleaned"
                    if col_cleaned in df_invoices_enhanced.columns:
                        optional_cols.append(pl.col(col_cleaned).alias(col.lower()))

                df_invoices_transformed = df_invoices_enhanced.with_columns(
                    [
                        pl.col("DOCUMENT_NUMBER_cleaned").alias("cash_invoice_number"),
                        pl.when(
                            pl.col("DOCUMENT_NUMBER_cleaned").str.starts_with("CN")
                            | (pl.col("amount_numeric") <= 0)
                        )
                        .then(pl.lit("CREDIT_NOTE"))
                        .otherwise(pl.lit("CASH_INVOICE"))
                        .alias("document_type"),
                        pl.col("CUS_CODE_cleaned").alias("customer_code"),
                        pl.col("CUSTOMER_NAME_cleaned").alias("customer_name"),
                        pl.col("amount_numeric").alias("amount"),
                        pl.col("amount_inclusive_numeric").alias("amount_inclusive"),
                        pl.col("amount_exclusive_numeric").alias("amount_exclusive"),
                        pl.col("vat_numeric").alias("vat_amount"),
                        pl.col("discount_numeric").alias("discount_amount"),
                        pl.col("transaction_date"),
                        pl.col("transaction_year"),
                        pl.col("transaction_month"),
                        pl.col("transaction_time"),
                        pl.lit(datetime.now()).alias("silver_load_timestamp"),
                        pl.lit(process_id).alias("silver_process_id"),
                    ]
                    + optional_cols
                )

                self.write_to_postgres(
                    df_invoices_transformed,
                    "silver_sales.summarized_cash_invoices",
                    "append",
                )

            # Transform and write credits with enhanced features
            if not df_credits.is_empty():
                # Get additional columns if they exist
                optional_cols = []
                for col in [
                    "BRANCH_NAME",
                    "BRANCH_CODE",
                    "SALES_USERNAME",
                    "USER_CODE",
                ]:
                    col_cleaned = col + "_cleaned"
                    if col_cleaned in df_credits.columns:
                        optional_cols.append(pl.col(col_cleaned).alias(col.lower()))

                df_credits_transformed = df_credits.with_columns(
                    [
                        pl.col("DOCUMENT_NUMBER_cleaned").alias(
                            "generated_credit_note_number"
                        ),
                        pl.col("CUS_CODE_cleaned").alias("customer_code"),
                        pl.col("CUSTOMER_NAME_cleaned").alias("customer_name"),
                        pl.col("amount_numeric").abs().alias("credit_amount"),
                        pl.col("amount_inclusive_numeric")
                        .abs()
                        .alias("credit_amount_inclusive"),
                        pl.col("amount_exclusive_numeric")
                        .abs()
                        .alias("credit_amount_exclusive"),
                        pl.col("vat_numeric").abs().alias("credit_vat_amount"),
                        pl.col("discount_numeric")
                        .abs()
                        .alias("credit_discount_amount"),
                        (
                            pl.col("amount_inclusive_numeric").abs()
                            - pl.col("amount_exclusive_numeric").abs()
                        ).alias("credit_calculated_total"),
                        pl.col("transaction_date"),
                        pl.col("transaction_year"),
                        pl.col("transaction_month"),
                        pl.lit(datetime.now()).alias("silver_load_timestamp"),
                        pl.lit(process_id).alias("silver_process_id"),
                        pl.when(pl.col("amount_numeric") == 0)
                        .then(pl.lit("ZERO_AMOUNT"))
                        .otherwise(pl.lit("CREDIT_NOTE"))
                        .alias("credit_note_type"),
                    ]
                    + optional_cols
                )

                self.write_to_postgres(
                    df_credits_transformed,
                    "silver_sales.generated_summarized_credit_notes",
                    "append",
                )

            # Update checkpoint and process log
            self.update_checkpoint(table_name, current_version, record_count)

            self._execute_sql(
                """UPDATE silver_sales.process_log 
                   SET completed_at = %s, records_processed = %s, records_valid = %s, 
                       credit_notes_separated = %s, potential_duplicates_detected = %s,
                       whitespace_issues_found = %s, last_processed_version = %s, status = 'COMPLETED'
                   WHERE process_id = %s""",
                (
                    datetime.now(),
                    record_count,
                    len(df_invoices),
                    len(df_credits),
                    potential_duplicates,
                    whitespace_issues,
                    current_version,
                    process_id,
                ),
            )

            logging.info(
                self.log_messages["summarized_results"].format(
                    new=record_count, invoices=len(df_invoices), credits=len(df_credits)
                )
            )
            return True

        except Exception as e:
            self._execute_sql(
                "UPDATE silver_sales.process_log SET completed_at = %s, status = 'FAILED', error_message = %s WHERE process_id = %s",
                (datetime.now(), str(e), process_id),
            )
            logging.error(
                self.log_messages["error"].format(layer="summarized", error=e)
            )
            return False

    def process_detailed_cash_invoices(self):
        """Process detailed cash invoices incrementally with enhanced features"""
        table_name = "bronze_sales_cash_invoices_detailed"
        delta_path = str(self.bronze_base_path / table_name)

        # Check if table exists
        if not Path(delta_path).exists():
            logging.info(f"ℹ️ {table_name} not found, skipping...")
            return True

        process_id = self._execute_sql(
            "INSERT INTO silver_sales.process_log (source_table, target_table, started_at, status) VALUES (%s, %s, %s, %s) RETURNING process_id",
            (
                table_name,
                "silver_sales.detailed_cash_invoices",
                datetime.now(),
                "STARTED",
            ),
        )[0][0]

        try:
            df_new, current_version, record_count = self.load_delta_incremental(
                delta_path, table_name
            )

            if df_new.is_empty():
                self._execute_sql(
                    "UPDATE silver_sales.process_log SET completed_at = %s, status = 'NO_NEW_DATA', last_processed_version = %s WHERE process_id = %s",
                    (datetime.now(), current_version, process_id),
                )
                return True

            whitespace_issues = self.detect_and_log_whitespace(
                df_new, table_name, process_id
            )
            potential_duplicates = self.detect_potential_duplicates(
                df_new, ["DOCUMENT_NUMBER", "ITEM_CODE", "TRN_DATE", "AMOUNT"]
            )

            if potential_duplicates > 0:
                self.log_data_quality_issue(
                    process_id,
                    table_name,
                    "ALL",
                    "POTENTIAL_DUPLICATES",
                    f"Found {potential_duplicates} potential duplicate line items",
                    potential_duplicates,
                    "",
                )

            df_cleaned = self._clean_data_with_polars(df_new)

            df_valid = df_cleaned.filter(
                (pl.col("DOCUMENT_NUMBER").is_not_null())
                & (pl.col("ITEM_CODE").is_not_null())
                & (pl.col("amount_numeric") > 0)
            )

            if not df_valid.is_empty():
                # Add enhanced calculations
                df_valid_enhanced = self._add_enhanced_calculations_detailed(df_valid)

                # Add per-record duplicate flags
                df_valid_enhanced = self._add_per_record_duplicate_flags(
                    df_valid_enhanced,
                    [
                        "cash_invoice_number",
                        "product_code",
                        "transaction_date",
                        "amount_numeric",
                    ],
                )

                # Get additional columns if they exist
                optional_cols = []
                for col in [
                    "BRANCH_NAME",
                    "BRANCH_CODE",
                    "SALES_USERNAME",
                    "USER_CODE",
                    "CUSTOMER_NAME",
                ]:
                    col_cleaned = col + "_cleaned"
                    if col_cleaned in df_valid_enhanced.columns:
                        optional_cols.append(pl.col(col_cleaned).alias(col.lower()))

                df_transformed = df_valid_enhanced.with_columns(
                    [
                        pl.col("DOCUMENT_NUMBER_cleaned").alias("cash_invoice_number"),
                        pl.col("ITEM_CODE_cleaned").alias("product_code"),
                        (
                            pl.col("DOCUMENT_NUMBER_cleaned")
                            + "_"
                            + pl.col("ITEM_CODE_cleaned")
                        ).alias("cash_invoice_line_identifier"),
                        pl.when(
                            pl.col("DOCUMENT_NUMBER_cleaned").str.starts_with("CN")
                            | (pl.col("amount_numeric") <= 0)
                        )
                        .then(pl.lit("CREDIT_NOTE"))
                        .otherwise(pl.lit("CASH_INVOICE"))
                        .alias("document_type"),
                        pl.col("CUS_CODE_cleaned").alias("customer_code"),
                        pl.col("amount_numeric").alias("amount"),
                        pl.col("amount_inclusive_numeric").alias("amount_inclusive"),
                        pl.col("amount_exclusive_numeric").alias("amount_exclusive"),
                        pl.col("vat_numeric").alias("vat_amount"),
                        pl.col("discount_numeric").alias("discount_amount"),
                        pl.col("transaction_date"),
                        pl.col("transaction_year"),
                        pl.col("transaction_month"),
                        pl.lit(datetime.now()).alias("silver_load_timestamp"),
                        pl.lit(process_id).alias("silver_process_id"),
                    ]
                    + optional_cols
                )

                self.write_to_postgres(
                    df_transformed, "silver_sales.detailed_cash_invoices", "append"
                )

            # Handle credit notes with enhanced features
            df_credits = df_cleaned.filter(
                (pl.col("DOCUMENT_NUMBER").is_not_null())
                & (pl.col("ITEM_CODE").is_not_null())
                & (pl.col("amount_numeric") <= 0)
            )

            if not df_credits.is_empty():
                # Get additional columns if they exist
                optional_cols = []
                for col in [
                    "BRANCH_NAME",
                    "BRANCH_CODE",
                    "SALES_USERNAME",
                    "USER_CODE",
                    "CUSTOMER_NAME",
                    "CUS_CODE",
                ]:
                    col_cleaned = col + "_cleaned"
                    if col_cleaned in df_credits.columns:
                        optional_cols.append(pl.col(col_cleaned).alias(col.lower()))

                df_credits_transformed = df_credits.with_columns(
                    [
                        pl.col("DOCUMENT_NUMBER_cleaned").alias(
                            "generated_credit_note_number"
                        ),
                        pl.col("ITEM_CODE_cleaned").alias("product_code"),
                        (
                            pl.col("DOCUMENT_NUMBER_cleaned")
                            + "_"
                            + pl.col("ITEM_CODE_cleaned")
                        ).alias("credit_line_identifier"),
                        pl.col("amount_numeric").abs().alias("credit_amount"),
                        pl.col("amount_inclusive_numeric")
                        .abs()
                        .alias("credit_amount_inclusive"),
                        pl.col("amount_exclusive_numeric")
                        .abs()
                        .alias("credit_amount_exclusive"),
                        pl.col("vat_numeric").abs().alias("credit_vat_amount"),
                        pl.col("discount_numeric")
                        .abs()
                        .alias("credit_discount_amount"),
                        (
                            pl.col("QUANTITY").cast(pl.Float64, strict=False)
                            * pl.col("ITEM_PRICE").cast(pl.Float64, strict=False)
                        )
                        .abs()
                        .alias("credit_calculated_line_total"),
                        pl.col("transaction_date"),
                        pl.col("transaction_year"),
                        pl.col("transaction_month"),
                        pl.lit(datetime.now()).alias("silver_load_timestamp"),
                        pl.lit(process_id).alias("silver_process_id"),
                        pl.when(pl.col("amount_numeric") == 0)
                        .then(pl.lit("ZERO_AMOUNT"))
                        .otherwise(pl.lit("CREDIT_NOTE"))
                        .alias("credit_note_type"),
                    ]
                    + optional_cols
                )

                self.write_to_postgres(
                    df_credits_transformed,
                    "silver_sales.generated_detailed_credit_notes",
                    "append",
                )

            self.update_checkpoint(table_name, current_version, record_count)

            self._execute_sql(
                """UPDATE silver_sales.process_log 
                   SET completed_at = %s, records_processed = %s, records_valid = %s,
                       credit_notes_separated = %s, potential_duplicates_detected = %s,
                       whitespace_issues_found = %s, last_processed_version = %s, status = 'COMPLETED'
                   WHERE process_id = %s""",
                (
                    datetime.now(),
                    record_count,
                    len(df_valid),
                    len(df_credits),
                    potential_duplicates,
                    whitespace_issues,
                    current_version,
                    process_id,
                ),
            )

            logging.info(
                self.log_messages["detailed_results"].format(
                    new=record_count, invoices=len(df_valid), credits=len(df_credits)
                )
            )
            return True

        except Exception as e:
            self._execute_sql(
                "UPDATE silver_sales.process_log SET completed_at = %s, status = 'FAILED', error_message = %s WHERE process_id = %s",
                (datetime.now(), str(e), process_id),
            )
            logging.error(self.log_messages["error"].format(layer="detailed", error=e))
            return False

    def run_incremental_pipeline(self):
        """Run incremental data processing pipeline with enhanced features"""
        logging.info(self.log_messages["start_pipeline"])
        logging.info(self.log_messages["preserve_note"])

        try:
            self.process_summarized_cash_invoices()
            self.process_detailed_cash_invoices()

            # Create daily sales summary
            logging.info("📈 Creating daily sales summary...")
            self.create_silver_daily_sales_summary()

            # Log overall data quality metrics
            self.log_overall_data_quality()

            logging.info(self.log_messages["success"])
            logging.info(self.log_messages["tip"])
            return True

        except Exception as e:
            logging.error(f"❌ Incremental pipeline failed: {e}")
            return False

    def create_silver_daily_sales_summary(self):
        """Create daily sales summary aggregation"""
        process_id = self._execute_sql(
            "INSERT INTO silver_sales.process_log (source_table, target_table, started_at, status) VALUES (%s, %s, %s, %s) RETURNING process_id",
            (
                "silver_sales tables",
                "silver_sales.daily_sales_summary",
                datetime.now(),
                "STARTED",
            ),
        )[0][0]

        try:
            # Check if silver tables exist
            summarized_exists = self._execute_sql(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'silver_sales' AND table_name = 'summarized_cash_invoices')"
            )[0][0]

            detailed_exists = self._execute_sql(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'silver_sales' AND table_name = 'detailed_cash_invoices')"
            )[0][0]

            if not summarized_exists and not detailed_exists:
                logging.info("ℹ️ No silver tables found for daily summary")
                self._execute_sql(
                    "UPDATE silver_sales.process_log SET completed_at = %s, status = 'NO_DATA' WHERE process_id = %s",
                    (datetime.now(), process_id),
                )
                return True

            # Build query parts with enhanced calculations
            summarized_query = (
                """
                SELECT 
                    transaction_date,
                    transaction_year,
                    transaction_month,
                    COALESCE(branch_name, 'Unknown') as branch_name,
                    COALESCE(branch_code, 'Unknown') as branch_code,
                    COALESCE(sales_username, 'Unknown') as sales_username,
                    COUNT(*) as invoice_count,
                    SUM(amount) as total_sales,
                    SUM(amount_inclusive) as total_sales_inclusive,
                    SUM(amount_exclusive) as total_sales_exclusive,
                    SUM(vat_amount) as total_vat,
                    SUM(discount_amount) as total_discount,
                    SUM(stg_vat_calculated) as total_calculated_vat,
                    AVG(stg_discount_percentage) as avg_discount_percentage,
                    COUNT(DISTINCT customer_code) as unique_customers,
                    'SUMMARIZED' as data_source
                FROM silver_sales.summarized_cash_invoices
                WHERE document_type = 'CASH_INVOICE'
                GROUP BY transaction_date, transaction_year, transaction_month, branch_name, branch_code, sales_username
            """
                if summarized_exists
                else ""
            )

            detailed_query = (
                """
                SELECT 
                    transaction_date,
                    transaction_year,
                    transaction_month,
                    COALESCE(branch_name, 'Unknown') as branch_name,
                    COALESCE(branch_code, 'Unknown') as branch_code,
                    COALESCE(sales_username, 'Unknown') as sales_username,
                    COUNT(DISTINCT cash_invoice_number) as invoice_count,
                    SUM(amount) as total_sales,
                    SUM(amount_inclusive) as total_sales_inclusive,
                    SUM(amount_exclusive) as total_sales_exclusive,
                    SUM(vat_amount) as total_vat,
                    SUM(discount_amount) as total_discount,
                    SUM(calculated_line_total) as total_calculated_line_amount,
                    AVG(line_discounted_product_unit_price) as avg_discounted_unit_price,
                    COUNT(DISTINCT customer_code) as unique_customers,
                    'DETAILED' as data_source
                FROM silver_sales.detailed_cash_invoices
                WHERE document_type = 'CASH_INVOICE'
                GROUP BY transaction_date, transaction_year, transaction_month, branch_name, branch_code, sales_username
            """
                if detailed_exists
                else ""
            )

            # Combine queries
            if summarized_exists and detailed_exists:
                combined_query = f"{summarized_query} UNION ALL {detailed_query}"
            elif summarized_exists:
                combined_query = summarized_query
            else:
                combined_query = detailed_query

            # Create or replace daily sales summary with enhanced metrics
            create_summary_query = f"""
                CREATE TABLE IF NOT EXISTS silver_sales.daily_sales_summary AS
                WITH daily_invoices AS (
                    {combined_query}
                ),
                daily_credits AS (
                    SELECT 
                        transaction_date,
                        COALESCE(branch_code, 'Unknown') as branch_code,
                        COUNT(*) as credit_note_count,
                        SUM(credit_amount) as total_credits,
                        SUM(credit_calculated_total) as total_calculated_credits
                    FROM (
                        SELECT transaction_date, branch_code, credit_amount, credit_calculated_total 
                        FROM silver_sales.generated_summarized_credit_notes
                        WHERE credit_note_type = 'CREDIT_NOTE'
                        UNION ALL
                        SELECT transaction_date, branch_code, credit_amount, credit_calculated_line_total 
                        FROM silver_sales.generated_detailed_credit_notes
                        WHERE credit_note_type = 'CREDIT_NOTE'
                    ) all_credits
                    GROUP BY transaction_date, branch_code
                )
                SELECT 
                    di.*,
                    COALESCE(dc.credit_note_count, 0) as credit_note_count,
                    COALESCE(dc.total_credits, 0) as total_credits,
                    COALESCE(dc.total_calculated_credits, 0) as total_calculated_credits,
                    di.total_sales - COALESCE(dc.total_credits, 0) as net_sales,
                    NOW() as summary_timestamp
                FROM daily_invoices di
                LEFT JOIN daily_credits dc 
                    ON di.transaction_date = dc.transaction_date 
                    AND di.branch_code = dc.branch_code
            """

            self._execute_sql("DROP TABLE IF EXISTS silver_sales.daily_sales_summary")
            self._execute_sql(create_summary_query)

            # Get summary count
            summary_count = self._execute_sql(
                "SELECT COUNT(*) FROM silver_sales.daily_sales_summary"
            )[0][0]

            self._execute_sql(
                """UPDATE silver_sales.process_log 
                   SET completed_at = %s, records_processed = %s, records_valid = %s, status = 'COMPLETED'
                   WHERE process_id = %s""",
                (datetime.now(), summary_count, summary_count, process_id),
            )

            logging.info(
                f"✅ Daily Sales Summary: {summary_count:,} records created with enhanced metrics"
            )
            return True

        except Exception as e:
            self._execute_sql(
                "UPDATE silver_sales.process_log SET completed_at = %s, status = 'FAILED', error_message = %s WHERE process_id = %s",
                (datetime.now(), str(e), process_id),
            )
            logging.error(f"❌ Failed to create daily sales summary: {e}")
            return False

    def log_overall_data_quality(self):
        """Log overall data quality metrics with enhanced features"""
        try:
            # Get process statistics
            stats = self._execute_sql(
                """
                SELECT 
                    COUNT(*) as total_processes,
                    SUM(records_processed) as total_records_processed,
                    SUM(records_valid) as total_valid_records,
                    SUM(credit_notes_separated) as total_credit_notes,
                    SUM(potential_duplicates_detected) as total_potential_duplicates,
                    SUM(whitespace_issues_found) as total_whitespace_issues
                FROM silver_sales.process_log 
                WHERE status = 'COMPLETED'
                AND target_table IN ('silver_sales.summarized_cash_invoices', 'silver_sales.detailed_cash_invoices')
                """
            )

            if stats and stats[0]:
                stat_row = stats[0]
                logging.info("📊 ENHANCED INCREMENTAL DATA QUALITY METRICS:")
                logging.info(f"   Total Processes: {stat_row[0]:,}")
                logging.info(f"   Total Records Processed: {stat_row[1] or 0:,}")
                logging.info(f"   Valid Records: {stat_row[2] or 0:,}")
                logging.info(f"   Credit Notes Separated: {stat_row[3] or 0:,}")
                logging.info(
                    f"   Potential Duplicates (for review): {stat_row[4] or 0:,}"
                )
                logging.info(f"   Columns with Whitespace Issues: {stat_row[5] or 0:,}")

            # Enhanced data quality issues summary
            issues = self._execute_sql(
                """
                SELECT issue_type, COUNT(*) as issue_count, SUM(affected_rows) as total_rows_affected
                FROM silver_sales.data_quality_log
                GROUP BY issue_type
                ORDER BY total_rows_affected DESC
                """
            )

            if issues:
                logging.info("🔍 ENHANCED DATA QUALITY ISSUES FOUND (for monitoring):")
                for issue_type, count, rows in issues:
                    if issue_type == "POTENTIAL_DUPLICATES":
                        logging.info(
                            f"   {issue_type}: {count} issues affecting {rows:,} rows - FOR BUSINESS REVIEW"
                        )
                    else:
                        logging.info(
                            f"   {issue_type}: {count} issues affecting {rows:,} rows"
                        )

            # Log enhanced feature usage
            enhanced_stats = self._execute_sql(
                """
                SELECT 
                    COUNT(*) as total_invoices,
                    COUNT(CASE WHEN stg_vat_calculated IS NOT NULL THEN 1 END) as invoices_with_vat_calc,
                    COUNT(CASE WHEN stg_discount_percentage IS NOT NULL THEN 1 END) as invoices_with_discount_calc,
                    COUNT(CASE WHEN amount_invalid_flag = 1 THEN 1 END) as invalid_amounts,
                    COUNT(CASE WHEN date_invalid_flag = 1 THEN 1 END) as invalid_dates
                FROM silver_sales.summarized_cash_invoices
                """
            )

            if enhanced_stats and enhanced_stats[0]:
                stats_row = enhanced_stats[0]
                logging.info("🧮 ENHANCED BUSINESS LOGIC METRICS:")
                logging.info(
                    f"   Invoices with VAT Calculations: {stats_row[1] or 0:,}"
                )
                logging.info(
                    f"   Invoices with Discount Calculations: {stats_row[2] or 0:,}"
                )
                logging.info(f"   Records with Invalid Amounts: {stats_row[3] or 0:,}")
                logging.info(f"   Records with Invalid Dates: {stats_row[4] or 0:,}")

        except Exception as e:
            logging.error(f"Failed to log overall data quality: {e}")

    def close(self):
        """Close database connection with cleanup"""
        if self.conn:
            self.conn.close()
        # Force garbage collection
        if (
            self.config.get("performance", {})
            .get("memory", {})
            .get("enable_garbage_collection", True)
        ):
            gc.collect()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Use silver_config.yaml (matches the config file name)
    transformer = SilverSalesTransformerDelta(config_path="silver_config.yaml")
    try:
        transformer.run_incremental_pipeline()
    finally:
        transformer.close()

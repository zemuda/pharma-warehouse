# silver_sales_transformer_transactional.py
import psycopg2
import polars as pl
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import logging
import re
from pathlib import Path
from silver_cash_invoices_config import (
    DELTA_PATHS,
    POSTGRES_CONFIG,
    SILVER_CONFIG,
    PROCESSING_CONFIG,
)


class SilverSalesTransformerTransactional:
    def __init__(self, db_params: Dict[str, str] = None):
        self.db_params = db_params or POSTGRES_CONFIG
        self.conn = self._connect_to_postgres()
        self._init_silver_schema()
        self._setup_platform_safe_logging()

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
        """Initialize enhanced silver sales schema with incremental processing support"""
        init_scripts = [
            f"CREATE SCHEMA IF NOT EXISTS {SILVER_CONFIG['schema']};",
            f"""
            CREATE TABLE IF NOT EXISTS {SILVER_CONFIG['schema']}.process_log (
                process_id SERIAL PRIMARY KEY,
                source_table VARCHAR,
                target_table VARCHAR,
                records_processed INTEGER,
                records_valid INTEGER,
                records_invalid INTEGER,
                credit_notes_separated INTEGER,
                potential_duplicates_detected INTEGER,
                whitespace_issues_found INTEGER,
                incremental_load BOOLEAN DEFAULT FALSE,
                last_processed_timestamp TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                status VARCHAR,
                error_message VARCHAR
            );
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {SILVER_CONFIG['schema']}.data_quality_log (
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
            f"""
            CREATE TABLE IF NOT EXISTS {SILVER_CONFIG['watermark_table']} (
                table_name VARCHAR PRIMARY KEY,
                last_processed_timestamp TIMESTAMP,
                last_processed_id BIGINT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
        ]

        for script in init_scripts:
            try:
                self._execute_sql(script)
            except Exception as e:
                logging.warning(f"Could not initialize silver sales schema: {e}")

    def _setup_platform_safe_logging(self):
        """Use platform-safe logging messages"""
        import os

        if os.name == "nt":
            self.log_messages = {
                "start_pipeline": "Starting transactional data cleaning pipeline...",
                "incremental_load": "🔄 INCREMENTAL LOAD: Processing new data since last run",
                "full_load": "🔄 FULL LOAD: Processing all available data",
                "preserve_note": "NOTE: All transactional data preserved - duplicates are monitored but not removed",
                "processing_summarized": "Processing summarized cash invoices...",
                "processing_detailed": "Processing detailed invoices...",
                "creating_summary": "Creating daily sales summary...",
                "success": "Transactional data cleaning completed successfully!",
                "tip": "All transactions preserved - check data quality logs for potential issues",
                "error": "Failed to create {layer} silver layer: {error}",
                "summarized_results": "Summarized Cash Invoices: {invoices:,} invoices, {credits:,} credit notes/zero amounts. Found {duplicates} potential duplicates for review, {whitespace} columns with whitespace issues",
                "detailed_results": "Detailed Invoices: {invoices:,} invoices, {credits:,} credit notes/zero amounts. Found {duplicates} potential duplicates for review, {whitespace} columns with whitespace issues",
                "daily_results": "Daily Sales Summary: {records:,} records created",
                "incremental_stats": "📊 Incremental Load: {new_records:,} new records processed from {source}",
            }
        else:
            self.log_messages = {
                "start_pipeline": "🔄 Starting transactional data cleaning pipeline...",
                "incremental_load": "🔄 INCREMENTAL LOAD: Processing new data since last run",
                "full_load": "🔄 FULL LOAD: Processing all available data",
                "preserve_note": "📝 NOTE: All transactional data preserved - duplicates are monitored but not removed",
                "processing_summarized": "📊 Processing summarized cash invoices...",
                "processing_detailed": "📋 Processing detailed invoices...",
                "creating_summary": "📈 Creating daily sales summary...",
                "success": "✅ Transactional data cleaning completed successfully!",
                "tip": "💡 All transactions preserved - check data quality logs for potential issues",
                "error": "❌ Failed to create {layer} silver layer: {error}",
                "summarized_results": "✅ Summarized Cash Invoices: {invoices:,} invoices, {credits:,} credit notes/zero amounts. Found {duplicates} potential duplicates for review, {whitespace} columns with whitespace issues",
                "detailed_results": "✅ Detailed Cash Invoices: {invoices:,} invoices, {credits:,} credit notes/zero amounts. Found {duplicates} potential duplicates for review, {whitespace} columns with whitespace issues",
                "daily_results": "✅ Daily Sales Summary: {records:,} records created",
                "incremental_stats": "📊 Incremental Load: {new_records:,} new records processed from {source}",
            }

    def _get_last_watermark(self, table_name: str) -> Tuple[datetime, int]:
        """Get the last processed timestamp and ID for incremental loading"""
        try:
            result = self._execute_sql(
                f"SELECT last_processed_timestamp, last_processed_id FROM {SILVER_CONFIG['watermark_table']} WHERE table_name = %s",
                (table_name,),
            )
            if result:
                return result[0][0], result[0][1]
            return None, None
        except Exception as e:
            logging.warning(f"Could not get watermark for {table_name}: {e}")
            return None, None

    def _update_watermark(
        self, table_name: str, timestamp: datetime, last_id: int = None
    ):
        """Update the watermark for incremental loading"""
        try:
            self._execute_sql(
                f"""
                INSERT INTO {SILVER_CONFIG['watermark_table']} 
                (table_name, last_processed_timestamp, last_processed_id, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (table_name) 
                DO UPDATE SET 
                    last_processed_timestamp = EXCLUDED.last_processed_timestamp,
                    last_processed_id = EXCLUDED.last_processed_id,
                    updated_at = EXCLUDED.updated_at
                """,
                (table_name, timestamp, last_id, datetime.now()),
            )
        except Exception as e:
            logging.error(f"Failed to update watermark for {table_name}: {e}")

    def _load_delta_table_incrementally(
        self, delta_path: Path, table_name: str
    ) -> pl.DataFrame:
        """Load Delta table incrementally based on watermark"""
        try:
            if not delta_path.exists():
                logging.warning(f"Delta table path does not exist: {delta_path}")
                return pl.DataFrame()

            # Get last watermark
            last_timestamp, last_id = self._get_last_watermark(table_name)

            if last_timestamp:
                # Incremental load
                logging.info(f"{self.log_messages['incremental_load']} - {table_name}")
                df = pl.read_delta(str(delta_path))

                # Filter for new records (assuming bronze_load_timestamp exists)
                if "bronze_load_timestamp" in df.columns:
                    new_records = df.filter(
                        pl.col("bronze_load_timestamp") > last_timestamp
                    )
                    logging.info(
                        self.log_messages["incremental_stats"].format(
                            new_records=new_records.height, source=table_name
                        )
                    )
                    return new_records
                else:
                    logging.warning(
                        f"bronze_load_timestamp not found in {table_name}, performing full load"
                    )
                    return pl.read_delta(str(delta_path))
            else:
                # Full load (first run)
                logging.info(f"{self.log_messages['full_load']} - {table_name}")
                return pl.read_delta(str(delta_path))

        except Exception as e:
            logging.error(f"Failed to load Delta table {delta_path}: {e}")
            return pl.DataFrame()

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
                f"""
                INSERT INTO {SILVER_CONFIG['schema']}.data_quality_log 
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

    def detect_and_log_trailing_whitespace(
        self, df: pl.DataFrame, table_name: str, process_id: int
    ) -> Dict[str, int]:
        """Detect and log columns with trailing whitespace using Polars"""
        whitespace_issues = {}

        try:
            if df.is_empty():
                return whitespace_issues

            # Check string columns for whitespace issues
            for col in df.columns:
                if df[col].dtype == pl.Utf8:
                    # Count rows with leading/trailing whitespace
                    whitespace_count = df.filter(
                        pl.col(col).str.strip_chars() != pl.col(col)
                    ).height

                    if whitespace_count > 0:
                        whitespace_issues[col] = whitespace_count

                        # Get sample data
                        sample_data = (
                            df.filter(pl.col(col).str.strip_chars() != pl.col(col))
                            .select(pl.col(col).head(5))
                            .to_series()
                            .to_list()
                        )

                        sample_str = ", ".join([str(s) for s in sample_data])

                        self.log_data_quality_issue(
                            process_id,
                            table_name,
                            col,
                            "TRAILING_WHITESPACE",
                            f"Found {whitespace_count} rows with trailing/leading whitespace",
                            whitespace_count,
                            sample_str,
                        )

        except Exception as e:
            logging.warning(f"Could not check whitespace for table {table_name}: {e}")

        return whitespace_issues

    def detect_potential_duplicates(
        self, df: pl.DataFrame, key_columns: List[str]
    ) -> int:
        """Detect potential duplicate transactions using Polars"""
        try:
            if df.is_empty():
                return 0

            # Count duplicates based on key columns
            duplicate_count = (
                df.group_by(key_columns)
                .agg(pl.count().alias("count"))
                .filter(pl.col("count") > 1)
                .height
            )

            return duplicate_count

        except Exception as e:
            logging.error(f"Failed to detect duplicates: {e}")
            return 0

    def detect_credit_notes(self, df: pl.DataFrame) -> Tuple[int, int]:
        """Detect credit notes and zero amounts using Polars"""
        try:
            if df.is_empty():
                return 0, 0

            total_records = df.height

            # Detect credit notes and zero amounts
            credit_notes_count = df.filter(
                (pl.col("DOCUMENT_NUMBER").str.starts_with("CN"))
                | (pl.col("AMOUNT").cast(pl.Float64) <= 0)
            ).height

            return total_records, credit_notes_count

        except Exception as e:
            logging.error(f"Error detecting credit notes: {e}")
            return 0, 0

    def _clean_data_with_polars(
        self, df: pl.DataFrame, table_type: str
    ) -> pl.DataFrame:
        """Clean and transform data using Polars"""
        if df.is_empty():
            return df

        # Common cleaning operations
        df_cleaned = df.with_columns(
            [
                # Trim string columns
                pl.col(pl.Utf8).str.strip_chars().name.suffix("_cleaned"),
                # Handle amounts - convert to numeric
                pl.col("AMOUNT").cast(pl.Float64).alias("amount_numeric"),
                pl.col("AMOUNT_INCLUSIVE")
                .cast(pl.Float64, strict=False)
                .alias("amount_inclusive_numeric"),
                pl.col("AMOUNT_EXCLUSIVE")
                .cast(pl.Float64, strict=False)
                .alias("amount_exclusive_numeric"),
                pl.col("VAT").cast(pl.Float64, strict=False).alias("vat_numeric"),
                pl.col("DISC").cast(pl.Float64, strict=False).alias("discount_numeric"),
            ]
        )

        # Handle date parsing
        if "TRN_DATE" in df_cleaned.columns:
            df_cleaned = df_cleaned.with_columns(
                [
                    pl.col("TRN_DATE")
                    .str.strptime(pl.Date, strict=False)
                    .alias("transaction_date"),
                    pl.col("TRN_DATE")
                    .str.strptime(pl.Datetime, strict=False)
                    .dt.time()
                    .alias("transaction_time"),
                ]
            )

        return df_cleaned

    def create_silver_summarized_cash_invoices(self, incremental: bool = True):
        """Create silver layer for summarized cash invoices using incremental Delta loading"""
        process_id = self._execute_sql(
            f"INSERT INTO {SILVER_CONFIG['schema']}.process_log (source_table, target_table, started_at, status, incremental_load) VALUES (%s, %s, %s, %s, %s) RETURNING process_id",
            (
                "bronze_sales_cash_invoices_summarized",
                f"{SILVER_CONFIG['schema']}.summarized_cash_invoices",
                datetime.now(),
                "STARTED",
                incremental,
            ),
        )[0][0]

        try:
            # Load data incrementally from Delta table
            df_bronze = self._load_delta_table_incrementally(
                DELTA_PATHS["summarized_cash_invoices"],
                "bronze_sales_cash_invoices_summarized",
            )

            if df_bronze.is_empty():
                logging.info("No new data found for summarized cash invoices")
                self._execute_sql(
                    f"""
                    UPDATE {SILVER_CONFIG['schema']}.process_log 
                    SET completed_at = %s, status = 'COMPLETED_NO_NEW_DATA'
                    WHERE process_id = %s
                    """,
                    (datetime.now(), process_id),
                )
                return True

            # Data quality monitoring
            whitespace_issues = self.detect_and_log_trailing_whitespace(
                df_bronze, "bronze_sales_cash_invoices_summarized", process_id
            )

            potential_duplicates = self.detect_potential_duplicates(
                df_bronze,
                ["DOCUMENT_NUMBER", "CUS_CODE", "TRN_DATE", "AMOUNT"],
            )

            if potential_duplicates > 0:
                self.log_data_quality_issue(
                    process_id,
                    "bronze_sales_cash_invoices_summarized",
                    "ALL",
                    "POTENTIAL_DUPLICATES",
                    f"Found {potential_duplicates} potential duplicate transactions for business review",
                    potential_duplicates,
                    "",
                )

            total_records, credit_notes_count = self.detect_credit_notes(df_bronze)

            # Clean and transform data
            df_cleaned = self._clean_data_with_polars(df_bronze, "summarized")

            # Separate positive amounts (invoices) and negative/zero amounts (credit notes)
            df_invoices = df_cleaned.filter(
                (pl.col("DOCUMENT_NUMBER").is_not_null())
                & (pl.col("amount_numeric") > 0)
            )

            df_credits = df_cleaned.filter(
                (pl.col("DOCUMENT_NUMBER").is_not_null())
                & (pl.col("amount_numeric") <= 0)
            )

            # Transform invoices and upsert to PostgreSQL
            if not df_invoices.is_empty():
                df_invoices_transformed = df_invoices.with_columns(
                    [
                        pl.col("DOCUMENT_NUMBER")
                        .str.strip_chars()
                        .alias("cash_invoice_number"),
                        pl.when(
                            pl.col("DOCUMENT_NUMBER").str.starts_with("CN")
                            | (pl.col("amount_numeric") <= 0)
                        )
                        .then(pl.lit("CREDIT_NOTE"))
                        .otherwise(pl.lit("CASH_INVOICE"))
                        .alias("document_type"),
                        pl.col("CUS_CODE").str.strip_chars().alias("customer_code"),
                        pl.col("CUSTOMER_NAME")
                        .str.strip_chars()
                        .alias("customer_name"),
                        pl.col("amount_numeric").alias("amount"),
                        pl.col("amount_inclusive_numeric").alias("amount_inclusive"),
                        pl.col("amount_exclusive_numeric").alias("amount_exclusive"),
                        pl.col("vat_numeric").alias("vat_amount"),
                        pl.col("discount_numeric").alias("discount_amount"),
                        pl.lit(datetime.now()).alias("silver_load_timestamp"),
                        pl.lit(process_id).alias("silver_process_id"),
                    ]
                )

                # Upsert to PostgreSQL
                self._upsert_dataframe(
                    df_invoices_transformed,
                    f"{SILVER_CONFIG['schema']}.summarized_cash_invoices",
                    ["cash_invoice_number", "customer_code", "transaction_date"],
                )

            # Transform credit notes and upsert
            if not df_credits.is_empty():
                df_credits_transformed = df_credits.with_columns(
                    [
                        pl.col("DOCUMENT_NUMBER")
                        .str.strip_chars()
                        .alias("generated_credit_note_number"),
                        pl.col("CUS_CODE").str.strip_chars().alias("customer_code"),
                        pl.col("CUSTOMER_NAME")
                        .str.strip_chars()
                        .alias("customer_name"),
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
                        pl.lit(datetime.now()).alias("silver_load_timestamp"),
                        pl.lit(process_id).alias("silver_process_id"),
                        pl.when(pl.col("amount_numeric") == 0)
                        .then(pl.lit("ZERO_AMOUNT"))
                        .otherwise(pl.lit("CREDIT_NOTE"))
                        .alias("credit_note_type"),
                    ]
                )

                self._upsert_dataframe(
                    df_credits_transformed,
                    f"{SILVER_CONFIG['schema']}.generated_summarized_credit_notes",
                    [
                        "generated_credit_note_number",
                        "customer_code",
                        "transaction_date",
                    ],
                )

            # Update watermark with latest timestamp
            latest_timestamp = (
                df_bronze["bronze_load_timestamp"].max()
                if "bronze_load_timestamp" in df_bronze.columns
                else datetime.now()
            )
            self._update_watermark(
                "bronze_sales_cash_invoices_summarized", latest_timestamp
            )

            # Get statistics
            invoice_stats = len(df_invoices)
            credit_stats = len(df_credits)

            # Update process log
            self._execute_sql(
                f"""
                UPDATE {SILVER_CONFIG['schema']}.process_log 
                SET completed_at = %s, 
                    records_processed = %s,
                    records_valid = %s,
                    credit_notes_separated = %s,
                    potential_duplicates_detected = %s,
                    whitespace_issues_found = %s,
                    last_processed_timestamp = %s,
                    status = 'COMPLETED'
                WHERE process_id = %s
                """,
                (
                    datetime.now(),
                    total_records,
                    invoice_stats,
                    credit_stats,
                    potential_duplicates,
                    len(whitespace_issues),
                    latest_timestamp,
                    process_id,
                ),
            )

            logging.info(
                self.log_messages["summarized_results"].format(
                    invoices=invoice_stats,
                    credits=credit_stats,
                    duplicates=potential_duplicates,
                    whitespace=len(whitespace_issues),
                )
            )
            return True

        except Exception as e:
            self._execute_sql(
                f"""
                UPDATE {SILVER_CONFIG['schema']}.process_log 
                SET completed_at = %s, status = 'FAILED', error_message = %s
                WHERE process_id = %s
                """,
                (datetime.now(), str(e), process_id),
            )
            logging.error(
                self.log_messages["error"].format(layer="summarized", error=e)
            )
            return False

    def _upsert_dataframe(
        self, df: pl.DataFrame, table_name: str, conflict_columns: List[str]
    ):
        """Upsert DataFrame to PostgreSQL table"""
        if df.is_empty():
            return

        try:
            # Create temporary table
            temp_table = f"temp_{table_name.split('.')[-1]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            # Write to temporary table
            df.write_database(temp_table, self.conn, if_table_exists="replace")

            # Build conflict resolution SQL
            conflict_clause = ", ".join(conflict_columns)
            update_columns = [col for col in df.columns if col not in conflict_columns]
            update_set = ", ".join(
                [f"{col} = EXCLUDED.{col}" for col in update_columns]
            )

            # Perform upsert
            upsert_sql = f"""
                INSERT INTO {table_name} 
                SELECT * FROM {temp_table}
                ON CONFLICT ({conflict_clause}) 
                DO UPDATE SET {update_set}
            """

            self._execute_sql(upsert_sql)

            # Drop temporary table
            self._execute_sql(f"DROP TABLE IF EXISTS {temp_table}")

        except Exception as e:
            logging.error(f"Failed to upsert data to {table_name}: {e}")
            raise

    def create_silver_detailed_cash_invoices(self, incremental: bool = True):
        """Create silver layer for detailed invoices using incremental Delta loading"""
        # Check if Delta table exists
        if not DELTA_PATHS["detailed_cash_invoices"].exists():
            logging.info("ℹ️ Detailed cash invoices Delta table not found, skipping...")
            return True

        process_id = self._execute_sql(
            f"INSERT INTO {SILVER_CONFIG['schema']}.process_log (source_table, target_table, started_at, status, incremental_load) VALUES (%s, %s, %s, %s, %s) RETURNING process_id",
            (
                "bronze_sales_cash_invoices_detailed",
                f"{SILVER_CONFIG['schema']}.detailed_cash_invoices",
                datetime.now(),
                "STARTED",
                incremental,
            ),
        )[0][0]

        try:
            # Load data incrementally from Delta table
            df_bronze = self._load_delta_table_incrementally(
                DELTA_PATHS["detailed_cash_invoices"],
                "bronze_sales_cash_invoices_detailed",
            )

            if df_bronze.is_empty():
                logging.info("No new data found for detailed cash invoices")
                self._execute_sql(
                    f"""
                    UPDATE {SILVER_CONFIG['schema']}.process_log 
                    SET completed_at = %s, status = 'COMPLETED_NO_NEW_DATA'
                    WHERE process_id = %s
                    """,
                    (datetime.now(), process_id),
                )
                return True

            # Data quality monitoring
            whitespace_issues = self.detect_and_log_trailing_whitespace(
                df_bronze, "bronze_sales_cash_invoices_detailed", process_id
            )

            potential_duplicates = self.detect_potential_duplicates(
                df_bronze,
                ["DOCUMENT_NUMBER", "ITEM_CODE", "TRN_DATE", "AMOUNT"],
            )

            if potential_duplicates > 0:
                self.log_data_quality_issue(
                    process_id,
                    "bronze_sales_cash_invoices_detailed",
                    "ALL",
                    "POTENTIAL_DUPLICATES",
                    f"Found {potential_duplicates} potential duplicate line items for business review",
                    potential_duplicates,
                    "",
                )

            total_records, credit_notes_count = self.detect_credit_notes(df_bronze)

            # Clean and transform data
            df_cleaned = self._clean_data_with_polars(df_bronze, "detailed")

            # Filter valid records
            df_valid = df_cleaned.filter(
                (pl.col("DOCUMENT_NUMBER").is_not_null())
                & (pl.col("ITEM_CODE").str.strip_chars().is_not_null())
                & (pl.col("amount_numeric") > 0)
            )

            # Transform and upsert invoices
            if not df_valid.is_empty():
                df_transformed = df_valid.with_columns(
                    [
                        pl.col("DOCUMENT_NUMBER")
                        .str.strip_chars()
                        .alias("cash_invoice_number"),
                        pl.col("ITEM_CODE").str.strip_chars().alias("product_code"),
                        (
                            pl.col("DOCUMENT_NUMBER").str.strip_chars()
                            + "_"
                            + pl.col("ITEM_CODE").str.strip_chars()
                        ).alias("cash_invoice_line_identifier"),
                        pl.when(
                            pl.col("DOCUMENT_NUMBER").str.starts_with("CN")
                            | (pl.col("amount_numeric") <= 0)
                        )
                        .then(pl.lit("CREDIT_NOTE"))
                        .otherwise(pl.lit("CASH_INVOICE"))
                        .alias("document_type"),
                        pl.col("CUS_CODE").str.strip_chars().alias("customer_code"),
                        pl.col("CUSTOMER_NAME")
                        .str.strip_chars()
                        .alias("customer_name"),
                        pl.col("amount_numeric").alias("amount"),
                        pl.lit(datetime.now()).alias("silver_load_timestamp"),
                        pl.lit(process_id).alias("silver_process_id"),
                    ]
                )

                self._upsert_dataframe(
                    df_transformed,
                    f"{SILVER_CONFIG['schema']}.detailed_cash_invoices",
                    ["cash_invoice_number", "product_code", "transaction_date"],
                )

            # Handle credit notes
            df_credits = df_cleaned.filter(
                (pl.col("DOCUMENT_NUMBER").is_not_null())
                & (pl.col("ITEM_CODE").str.strip_chars().is_not_null())
                & (pl.col("amount_numeric") <= 0)
            )

            if not df_credits.is_empty():
                df_credits_transformed = df_credits.with_columns(
                    [
                        pl.col("DOCUMENT_NUMBER")
                        .str.strip_chars()
                        .alias("generated_credit_note_number"),
                        pl.col("ITEM_CODE").str.strip_chars().alias("product_code"),
                        (
                            pl.col("DOCUMENT_NUMBER").str.strip_chars()
                            + "_"
                            + pl.col("ITEM_CODE").str.strip_chars()
                        ).alias("credit_line_identifier"),
                        pl.col("amount_numeric").abs().alias("credit_amount"),
                        pl.lit(datetime.now()).alias("silver_load_timestamp"),
                        pl.lit(process_id).alias("silver_process_id"),
                        pl.when(pl.col("amount_numeric") == 0)
                        .then(pl.lit("ZERO_AMOUNT"))
                        .otherwise(pl.lit("CREDIT_NOTE"))
                        .alias("credit_note_type"),
                    ]
                )

                self._upsert_dataframe(
                    df_credits_transformed,
                    f"{SILVER_CONFIG['schema']}.generated_detailed_credit_notes",
                    [
                        "generated_credit_note_number",
                        "product_code",
                        "transaction_date",
                    ],
                )

            # Update watermark
            latest_timestamp = (
                df_bronze["bronze_load_timestamp"].max()
                if "bronze_load_timestamp" in df_bronze.columns
                else datetime.now()
            )
            self._update_watermark(
                "bronze_sales_cash_invoices_detailed", latest_timestamp
            )

            # Get statistics
            invoice_stats = len(df_valid)
            credit_stats = len(df_credits)

            self._execute_sql(
                f"""
                UPDATE {SILVER_CONFIG['schema']}.process_log 
                SET completed_at = %s, 
                    records_processed = %s,
                    records_valid = %s,
                    credit_notes_separated = %s,
                    potential_duplicates_detected = %s,
                    whitespace_issues_found = %s,
                    last_processed_timestamp = %s,
                    status = 'COMPLETED'
                WHERE process_id = %s
                """,
                (
                    datetime.now(),
                    total_records,
                    invoice_stats,
                    credit_stats,
                    potential_duplicates,
                    len(whitespace_issues),
                    latest_timestamp,
                    process_id,
                ),
            )

            logging.info(
                self.log_messages["detailed_results"].format(
                    invoices=invoice_stats,
                    credits=credit_stats,
                    duplicates=potential_duplicates,
                    whitespace=len(whitespace_issues),
                )
            )
            return True

        except Exception as e:
            self._execute_sql(
                f"""
                UPDATE {SILVER_CONFIG['schema']}.process_log 
                SET completed_at = %s, status = 'FAILED', error_message = %s
                WHERE process_id = %s
                """,
                (datetime.now(), str(e), process_id),
            )
            logging.error(self.log_messages["error"].format(layer="detailed", error=e))
            return False

    def run_transactional_cleaning(self, incremental: bool = True):
        """Run transactional data cleaning pipeline with incremental processing"""
        logging.info(self.log_messages["start_pipeline"])
        if incremental:
            logging.info(self.log_messages["incremental_load"])
        else:
            logging.info(self.log_messages["full_load"])
        logging.info(self.log_messages["preserve_note"])

        try:
            # Process summarized cash invoices
            logging.info(self.log_messages["processing_summarized"])
            self.create_silver_summarized_cash_invoices(incremental)

            # Process detailed invoices
            logging.info(self.log_messages["processing_detailed"])
            self.create_silver_detailed_cash_invoices(incremental)

            # Log overall data quality metrics
            self.log_overall_data_quality()

            logging.info(self.log_messages["success"])
            logging.info(self.log_messages["tip"])
            return True

        except Exception as e:
            logging.error(f"❌ Transactional data cleaning failed: {e}")
            return False

    def log_overall_data_quality(self):
        """Log overall data quality metrics"""
        try:
            # Get process statistics
            stats = self._execute_sql(
                f"""
                SELECT 
                    COUNT(*) as total_processes,
                    SUM(records_processed) as total_records_processed,
                    SUM(records_valid) as total_valid_records,
                    SUM(credit_notes_separated) as total_credit_notes,
                    SUM(potential_duplicates_detected) as total_potential_duplicates,
                    SUM(whitespace_issues_found) as total_whitespace_issues
                FROM {SILVER_CONFIG['schema']}.process_log 
                WHERE status = 'COMPLETED'
            """
            )[0]

            logging.info("📊 TRANSACTIONAL DATA QUALITY METRICS:")
            logging.info(f"   Total Records Processed: {stats[1]:,}")
            logging.info(f"   Valid Records: {stats[2]:,}")
            logging.info(f"   Credit Notes Separated: {stats[3]:,}")
            logging.info(f"   Potential Duplicates (for review): {stats[4]:,}")
            logging.info(f"   Columns with Whitespace Issues: {stats[5]:,}")

            # Data quality issues summary
            issues = self._execute_sql(
                f"""
                SELECT issue_type, COUNT(*) as issue_count, SUM(affected_rows) as total_rows_affected
                FROM {SILVER_CONFIG['schema']}.data_quality_log
                GROUP BY issue_type
                ORDER BY total_rows_affected DESC
            """
            )

            if issues:
                logging.info("🔍 DATA QUALITY ISSUES FOUND (for monitoring):")
                for issue_type, count, rows in issues:
                    if issue_type == "POTENTIAL_DUPLICATES":
                        logging.info(
                            f"   {issue_type}: {count} issues affecting {rows:,} rows - FOR BUSINESS REVIEW"
                        )
                    else:
                        logging.info(
                            f"   {issue_type}: {count} issues affecting {rows:,} rows"
                        )

        except Exception as e:
            logging.error(f"Failed to log overall data quality: {e}")

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()

# silver_sales_transformer_transactional.py
import psycopg2
import polars as pl
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging
import re


class SilverSalesTransformerTransactional:
    def __init__(self, db_params: Dict[str, str] = None):
        self.db_params = db_params or {
            "host": "localhost",
            "database": "sales_db",
            "user": "postgres",
            "password": "password",
            "port": "5432",
        }
        self.conn = self._connect_to_postgres()
        self._init_silver_schema()

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
        ]

        for script in init_scripts:
            try:
                self._execute_sql(script)
            except Exception as e:
                logging.warning(f"Could not initialize silver sales schema: {e}")

    def update_schema(self):
        """Update database schema to include new columns"""
        try:
            # Check if the new column exists in process_log table
            column_exists = self._execute_sql(
                """
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'silver_sales' 
                AND table_name = 'process_log' 
                AND column_name = 'potential_duplicates_detected'
            """
            )

            if not column_exists:
                logging.info("Updating process_log table schema...")
                self._execute_sql(
                    """
                    ALTER TABLE silver_sales.process_log 
                    ADD COLUMN potential_duplicates_detected INTEGER
                """
                )
                logging.info(
                    "Added potential_duplicates_detected column to process_log"
                )

            self._setup_platform_safe_logging()

        except Exception as e:
            logging.warning(f"Schema update may have failed: {e}")

    def _setup_platform_safe_logging(self):
        """Use platform-safe logging messages"""
        import os

        if os.name == "nt":
            self.log_messages = {
                "start_pipeline": "Starting transactional data cleaning pipeline...",
                "preserve_note": "NOTE: All transactional data preserved - duplicates are monitored but not removed",
                "processing_summarized": "Processing summarized cash invoices...",
                "processing_detailed": "Processing detailed invoices...",
                "creating_summary": "Creating daily sales summary...",
                "success": "Transactional data cleaning completed successfully!",
                "tip": "All transactions preserved - check data quality logs for potential issues",
                "error": "Failed to create {layer} silver layer: {error}",
                "summarized_results": "summarized cash invoices: {invoices:,} invoices, {credits:,} credit notes/zero amounts. Found {duplicates} potential duplicates for review, {whitespace} columns with whitespace issues",
                "detailed_results": "Detailed Invoices: {invoices:,} invoices, {credits:,} credit notes/zero amounts. Found {duplicates} potential duplicates for review, {whitespace} columns with whitespace issues",
                "daily_results": "Daily Sales Summary: {records:,} records created",
            }
        else:
            self.log_messages = {
                "start_pipeline": "🔄 Starting transactional data cleaning pipeline...",
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
            }

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

    def _load_table_to_polars(self, table_name: str) -> pl.DataFrame:
        """Load table data into Polars DataFrame"""
        try:
            return pl.read_database(f"SELECT * FROM {table_name}", self.conn)
        except Exception as e:
            logging.error(f"Failed to load table {table_name} to Polars: {e}")
            return pl.DataFrame()

    def detect_and_log_trailing_whitespace(
        self, table_name: str, process_id: int
    ) -> Dict[str, int]:
        """Detect and log columns with trailing whitespace using Polars"""
        whitespace_issues = {}

        try:
            # Load table into Polars
            df = self._load_table_to_polars(table_name)
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
        self, table_name: str, key_columns: List[str]
    ) -> int:
        """Detect potential duplicate transactions using Polars"""
        try:
            df = self._load_table_to_polars(table_name)
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
            logging.error(f"Failed to detect duplicates in {table_name}: {e}")
            return 0

    def detect_credit_notes(self, table_name: str) -> Tuple[int, int]:
        """Detect credit notes and zero amounts using Polars"""
        try:
            df = self._load_table_to_polars(table_name)
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
            logging.error(f"Error detecting credit notes in {table_name}: {e}")
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
                .cast(pl.Float64)
                .alias("amount_inclusive_numeric"),
                pl.col("AMOUNT_EXCLUSIVE")
                .cast(pl.Float64)
                .alias("amount_exclusive_numeric"),
                pl.col("VAT").cast(pl.Float64).alias("vat_numeric"),
                pl.col("DISC").cast(pl.Float64).alias("discount_numeric"),
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

    def create_silver_summarized_cash_invoices(self):
        """Create silver layer for summarized cash invoices using Polars"""
        process_id = self._execute_sql(
            "INSERT INTO silver_sales.process_log (source_table, target_table, started_at, status) VALUES (%s, %s, %s, %s) RETURNING process_id",
            (
                "bronze_sales_cash_invoices_summarized",
                "silver_sales.summarized_cash_invoices",
                datetime.now(),
                "STARTED",
            ),
        )[0][0]

        try:
            # Data quality monitoring
            whitespace_issues = self.detect_and_log_trailing_whitespace(
                "bronze_sales_cash_invoices_summarized", process_id
            )

            potential_duplicates = self.detect_potential_duplicates(
                "bronze_sales_cash_invoices_summarized",
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

            total_records, credit_notes_count = self.detect_credit_notes(
                "bronze_sales_cash_invoices_summarized"
            )

            # Load and transform data with Polars
            df_bronze = self._load_table_to_polars(
                "bronze_sales_cash_invoices_summarized"
            )

            if not df_bronze.is_empty():
                # Clean data
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

                # Transform invoices
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
                            pl.col("amount_inclusive_numeric").alias(
                                "amount_inclusive"
                            ),
                            pl.col("amount_exclusive_numeric").alias(
                                "amount_exclusive"
                            ),
                            pl.col("vat_numeric").alias("vat_amount"),
                            pl.col("discount_numeric").alias("discount_amount"),
                            pl.lit(datetime.now()).alias("silver_load_timestamp"),
                            pl.lit(process_id).alias("silver_process_id"),
                        ]
                    )

                    # Write to PostgreSQL
                    df_invoices_transformed.write_database(
                        "silver_sales.summarized_cash_invoices",
                        self.conn,
                        if_table_exists="replace",
                    )

                # Transform credit notes
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

                    df_credits_transformed.write_database(
                        "silver_sales.generated_summarized_credit_notes",
                        self.conn,
                        if_table_exists="replace",
                    )

            # Get statistics
            invoice_stats = self._execute_sql(
                "SELECT COUNT(*) as invoice_count FROM silver_sales.summarized_cash_invoices"
            )[0][0]

            credit_stats = self._execute_sql(
                "SELECT COUNT(*) as credit_note_count FROM silver_sales.generated_summarized_credit_notes"
            )[0][0]

            # Update process log
            self._execute_sql(
                """
                UPDATE silver_sales.process_log 
                SET completed_at = %s, 
                    records_processed = %s,
                    records_valid = %s,
                    credit_notes_separated = %s,
                    potential_duplicates_detected = %s,
                    whitespace_issues_found = %s,
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
                """
                UPDATE silver_sales.process_log 
                SET completed_at = %s, status = 'FAILED', error_message = %s
                WHERE process_id = %s
                """,
                (datetime.now(), str(e), process_id),
            )
            logging.error(
                self.log_messages["error"].format(layer="summarized", error=e)
            )
            return False

    def create_silver_detailed_cash_invoices(self):
        """Create silver layer for detailed invoices using Polars"""
        # Check if detailed table exists
        table_exists = self._execute_sql(
            """
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = 'bronze_sales_cash_invoices_detailed'
            """
        )

        if not table_exists:
            logging.info("ℹ️ Detailed cash invoices table not found, skipping...")
            return True

        process_id = self._execute_sql(
            "INSERT INTO silver_sales.process_log (source_table, target_table, started_at, status) VALUES (%s, %s, %s, %s) RETURNING process_id",
            (
                "bronze_sales_cash_invoices_detailed",
                "silver_sales.detailed_cash_invoices",
                datetime.now(),
                "STARTED",
            ),
        )[0][0]

        try:
            # Data quality monitoring
            whitespace_issues = self.detect_and_log_trailing_whitespace(
                "bronze_sales_cash_invoices_detailed", process_id
            )

            potential_duplicates = self.detect_potential_duplicates(
                "bronze_sales_cash_invoices_detailed",
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

            total_records, credit_notes_count = self.detect_credit_notes(
                "bronze_sales_cash_invoices_detailed"
            )

            # Load and transform with Polars
            df_bronze = self._load_table_to_polars(
                "bronze_sales_cash_invoices_detailed"
            )

            if not df_bronze.is_empty():
                df_cleaned = self._clean_data_with_polars(df_bronze, "detailed")

                # Filter valid records
                df_valid = df_cleaned.filter(
                    (pl.col("DOCUMENT_NUMBER").is_not_null())
                    & (pl.col("ITEM_CODE").str.strip_chars().is_not_null())
                    & (pl.col("amount_numeric") > 0)
                )

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

                    df_transformed.write_database(
                        "silver_sales.detailed_cash_invoices",
                        self.conn,
                        if_table_exists="replace",
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

                    df_credits_transformed.write_database(
                        "silver_sales.generated_detailed_credit_notes",
                        self.conn,
                        if_table_exists="replace",
                    )

            # Get statistics
            invoice_stats = self._execute_sql(
                "SELECT COUNT(*) as invoice_count FROM silver_sales.detailed_cash_invoices"
            )[0][0]

            credit_stats = self._execute_sql(
                "SELECT COUNT(*) as credit_note_count FROM silver_sales.generated_detailed_credit_notes"
            )[0][0]

            self._execute_sql(
                """
                UPDATE silver_sales.process_log 
                SET completed_at = %s, 
                    records_processed = %s,
                    records_valid = %s,
                    credit_notes_separated = %s,
                    potential_duplicates_detected = %s,
                    whitespace_issues_found = %s,
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
                """
                UPDATE silver_sales.process_log 
                SET completed_at = %s, status = 'FAILED', error_message = %s
                WHERE process_id = %s
                """,
                (datetime.now(), str(e), process_id),
            )
            logging.error(self.log_messages["error"].format(layer="detailed", error=e))
            return False

    def create_silver_daily_sales_summary(self):
        """Create daily sales summary using Polars"""
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
            # Load data from silver tables using Polars
            df_summarized = self._load_table_to_polars(
                "silver_sales.summarized_cash_invoices"
            )
            df_detailed = self._load_table_to_polars(
                "silver_sales.detailed_cash_invoices"
            )
            df_summarized_credits = self._load_table_to_polars(
                "silver_sales.generated_summarized_credit_notes"
            )
            df_detailed_credits = self._load_table_to_polars(
                "silver_sales.generated_detailed_credit_notes"
            )

            # Process summarized invoices
            if not df_summarized.is_empty():
                summarized_daily = (
                    df_summarized.group_by(
                        [
                            "transaction_date",
                            "transaction_year",
                            "transaction_month",
                            "branch_name",
                            "branch_code",
                            "sales_username",
                        ]
                    )
                    .agg(
                        [
                            pl.count().alias("invoice_count"),
                            pl.col("amount").sum().alias("total_sales"),
                            pl.col("amount_inclusive")
                            .sum()
                            .alias("total_sales_inclusive"),
                            pl.col("amount_exclusive")
                            .sum()
                            .alias("total_sales_exclusive"),
                            pl.col("vat_amount").sum().alias("total_vat"),
                            pl.col("discount_amount").sum().alias("total_discount"),
                            pl.col("customer_code")
                            .n_unique()
                            .alias("unique_customers"),
                        ]
                    )
                    .with_columns(pl.lit("SUMMARIZED").alias("data_source"))
                )

            # Process detailed invoices
            if not df_detailed.is_empty():
                detailed_daily = (
                    df_detailed.group_by(
                        [
                            "transaction_date",
                            "transaction_year",
                            "transaction_month",
                            "branch_name",
                            "branch_code",
                            "sales_username",
                        ]
                    )
                    .agg(
                        [
                            pl.col("cash_invoice_number")
                            .n_unique()
                            .alias("invoice_count"),
                            pl.col("amount").sum().alias("total_sales"),
                            pl.col("amount_inclusive")
                            .sum()
                            .alias("total_sales_inclusive"),
                            pl.col("amount_exclusive")
                            .sum()
                            .alias("total_sales_exclusive"),
                            pl.col("vat_amount").sum().alias("total_vat"),
                            pl.col("discount_amount").sum().alias("total_discount"),
                            pl.col("customer_code")
                            .n_unique()
                            .alias("unique_customers"),
                        ]
                    )
                    .with_columns(pl.lit("DETAILED").alias("data_source"))
                )

            # Combine data
            daily_invoices = pl.concat([summarized_daily, detailed_daily])

            # Process credit notes
            credits_list = []
            if not df_summarized_credits.is_empty():
                summarized_credits = (
                    df_summarized_credits.filter(
                        pl.col("credit_note_type") == "CREDIT_NOTE"
                    )
                    .group_by(["transaction_date", "branch_code"])
                    .agg(
                        [
                            pl.count().alias("credit_note_count"),
                            pl.col("credit_amount").sum().alias("total_credits"),
                        ]
                    )
                )
                credits_list.append(summarized_credits)

            if not df_detailed_credits.is_empty():
                detailed_credits = (
                    df_detailed_credits.filter(
                        pl.col("credit_note_type") == "CREDIT_NOTE"
                    )
                    .group_by(["transaction_date", "branch_code"])
                    .agg(
                        [
                            pl.count().alias("credit_note_count"),
                            pl.col("credit_amount").sum().alias("total_credits"),
                        ]
                    )
                )
                credits_list.append(detailed_credits)

            daily_credits = pl.concat(credits_list) if credits_list else None

            # Create final summary
            if daily_credits is not None:
                final_summary = daily_invoices.join(
                    daily_credits,
                    left_on=["transaction_date", "branch_code"],
                    right_on=["transaction_date", "branch_code"],
                    how="left",
                ).with_columns(
                    [
                        pl.col("credit_note_count").fill_null(0),
                        pl.col("total_credits").fill_null(0),
                        (pl.col("total_sales") - pl.col("total_credits")).alias(
                            "net_sales"
                        ),
                        pl.lit(datetime.now()).alias("summary_timestamp"),
                    ]
                )
            else:
                final_summary = daily_invoices.with_columns(
                    [
                        pl.lit(0).alias("credit_note_count"),
                        pl.lit(0).alias("total_credits"),
                        pl.col("total_sales").alias("net_sales"),
                        pl.lit(datetime.now()).alias("summary_timestamp"),
                    ]
                )

            # Write to database
            if not final_summary.is_empty():
                final_summary.write_database(
                    "silver_sales.daily_sales_summary",
                    self.conn,
                    if_table_exists="replace",
                )

            summary_stats = self._execute_sql(
                "SELECT COUNT(*) FROM silver_sales.daily_sales_summary"
            )[0][0]

            self._execute_sql(
                """
                UPDATE silver_sales.process_log 
                SET completed_at = %s, 
                    records_processed = %s,
                    records_valid = %s,
                    status = 'COMPLETED'
                WHERE process_id = %s
                """,
                (datetime.now(), summary_stats, summary_stats, process_id),
            )

            logging.info(
                self.log_messages["daily_results"].format(records=summary_stats)
            )
            return True

        except Exception as e:
            self._execute_sql(
                """
                UPDATE silver_sales.process_log 
                SET completed_at = %s, status = 'FAILED', error_message = %s
                WHERE process_id = %s
                """,
                (datetime.now(), str(e), process_id),
            )
            logging.error(f"❌ Failed to create daily sales summary: {e}")
            return False

    def run_transactional_cleaning(self):
        """Run transactional data cleaning pipeline"""
        logging.info(self.log_messages["start_pipeline"])
        logging.info(self.log_messages["preserve_note"])

        try:
            # Process summarized cash invoices
            logging.info(self.log_messages["processing_summarized"])
            self.create_silver_summarized_cash_invoices()

            # Process detailed invoices
            logging.info(self.log_messages["processing_detailed"])
            self.create_silver_detailed_cash_invoices()

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
                """
                SELECT issue_type, COUNT(*) as issue_count, SUM(affected_rows) as total_rows_affected
                FROM silver_sales.data_quality_log
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

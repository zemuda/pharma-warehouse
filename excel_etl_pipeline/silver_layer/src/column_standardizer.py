# src/column_standardizer.py
"""
Column Standardization Engine
Handles column renaming and standardization across all table types
"""
import polars as pl
from typing import Dict, List
import yaml
import logging

logger = logging.getLogger(__name__)


class ColumnStandardizer:
    """Standardize column names across table types"""

    def __init__(self, mapping_file: str):
        with open(mapping_file, "r") as f:
            self.mappings = yaml.safe_load(f)

    def standardize_columns(self, df: pl.DataFrame, table_type: str) -> pl.DataFrame:
        """Standardize column names based on table type"""

        if table_type not in self.mappings:
            logger.warning(f"⚠️  No column mapping found for table type: {table_type}")
            return df

        mapping = self.mappings[table_type]

        # Only rename columns that exist in the DataFrame
        rename_map = {
            old_col: new_col
            for old_col, new_col in mapping.items()
            if old_col in df.columns
        }

        if rename_map:
            df = df.rename(rename_map)
            logger.info(
                f"✏️  Standardized {len(rename_map)} column names for {table_type}"
            )

            # Log sample of renamed columns
            sample = list(rename_map.items())[:5]
            for old, new in sample:
                logger.debug(f"   {old} → {new}")

        # Convert all columns to snake_case
        df = self._to_snake_case(df)

        return df

    def _to_snake_case(self, df: pl.DataFrame) -> pl.DataFrame:
        """Convert all column names to snake_case"""
        rename_map = {}

        for col in df.columns:
            snake_col = col.lower()
            snake_col = snake_col.replace(" ", "_").replace("-", "_")
            # Remove multiple underscores
            while "__" in snake_col:
                snake_col = snake_col.replace("__", "_")
            snake_col = snake_col.strip("_")

            if snake_col != col:
                rename_map[col] = snake_col

        if rename_map:
            df = df.rename(rename_map)

        return df

    def get_expected_columns(self, table_type: str) -> List[str]:
        """Get expected columns after standardization"""
        if table_type in self.mappings:
            return list(self.mappings[table_type].values())
        return []


class DataTransformer:
    """Apply business transformations to data"""

    @staticmethod
    def add_calculated_fields_invoice_header(df: pl.DataFrame) -> pl.DataFrame:
        """Add calculated fields for invoice headers"""

        logger.info("➕ Adding calculated fields for invoice headers...")

        # Date components (if not already present)
        if "transaction_date" in df.columns:
            df = df.with_columns(
                [
                    pl.col("transaction_date").dt.year().alias("transaction_year"),
                    pl.col("transaction_date").dt.month().alias("transaction_month"),
                    pl.col("transaction_date").dt.day().alias("transaction_day"),
                    pl.col("transaction_date").dt.hour().alias("transaction_hour"),
                    pl.col("transaction_date")
                    .dt.strftime("%A")
                    .alias("transaction_day_of_week"),
                ]
            )

        # Amount calculations
        if "total_amount" in df.columns and "discount_amount" in df.columns:
            df = df.with_columns(
                [
                    (
                        pl.col("total_amount") - pl.col("discount_amount").fill_null(0)
                    ).alias("net_amount")
                ]
            )

        if "total_amount" in df.columns and "tax_amount" in df.columns:
            df = df.with_columns(
                [
                    (pl.col("total_amount") + pl.col("tax_amount").fill_null(0)).alias(
                        "gross_amount"
                    )
                ]
            )

        # Percentage calculations
        if "discount_amount" in df.columns and "amount_excluding_tax" in df.columns:
            df = df.with_columns(
                [
                    (pl.col("discount_amount") / pl.col("amount_excluding_tax") * 100)
                    .fill_null(0)
                    .alias("discount_percentage")
                ]
            )

        if "tax_amount" in df.columns and "amount_excluding_tax" in df.columns:
            df = df.with_columns(
                [
                    (pl.col("tax_amount") / pl.col("amount_excluding_tax") * 100)
                    .fill_null(0)
                    .alias("tax_percentage")
                ]
            )

        # Business flags
        if "discount_amount" in df.columns:
            df = df.with_columns(
                [(pl.col("discount_amount") > 0).alias("has_discount")]
            )

        if "tax_amount" in df.columns:
            df = df.with_columns([(pl.col("tax_amount") > 0).alias("has_tax")])

        if "customer_name" in df.columns:
            df = df.with_columns(
                [
                    pl.col("customer_name")
                    .str.contains("(?i)cash customer")
                    .alias("is_cash_customer")
                ]
            )

        if "total_amount" in df.columns:
            df = df.with_columns(
                [(pl.col("total_amount") > 10000).alias("is_high_value_transaction")]
            )

        logger.info("✅ Calculated fields added")
        return df

    @staticmethod
    def add_calculated_fields_invoice_detail(df: pl.DataFrame) -> pl.DataFrame:
        """Add calculated fields for invoice details"""

        logger.info("➕ Adding calculated fields for invoice details...")

        # Line total calculations
        if "quantity_sold" in df.columns and "unit_price" in df.columns:
            df = df.with_columns(
                [
                    (pl.col("quantity_sold") * pl.col("unit_price")).alias(
                        "calculated_line_total"
                    )
                ]
            )

        # Discount calculations
        if (
            "calculated_line_total" in df.columns
            and "line_discount_percentage" in df.columns
        ):
            df = df.with_columns(
                [
                    (
                        pl.col("calculated_line_total")
                        * pl.col("line_discount_percentage")
                        / 100
                    )
                    .fill_null(0)
                    .alias("calculated_discount_amount")
                ]
            )

        # Net line amount
        if (
            "calculated_line_total" in df.columns
            and "calculated_discount_amount" in df.columns
        ):
            df = df.with_columns(
                [
                    (
                        pl.col("calculated_line_total")
                        - pl.col("calculated_discount_amount")
                    ).alias("net_line_amount")
                ]
            )

        # Variance analysis
        if "line_total_amount" in df.columns and "calculated_line_total" in df.columns:
            df = df.with_columns(
                [
                    (pl.col("line_total_amount") - pl.col("calculated_line_total"))
                    .abs()
                    .alias("amount_variance"),
                ]
            )

            df = df.with_columns(
                [(pl.col("amount_variance") > 0.01).alias("has_amount_variance")]
            )

        # Product categorization
        if "product_description" in df.columns:
            df = df.with_columns(
                [
                    pl.when(
                        pl.col("product_description").str.contains(
                            "(?i)inject|inj|syringe"
                        )
                    )
                    .then(pl.lit("INJECTABLES"))
                    .when(
                        pl.col("product_description").str.contains(
                            "(?i)tab|caps|caplet"
                        )
                    )
                    .then(pl.lit("TABLETS_CAPSULES"))
                    .when(
                        pl.col("product_description").str.contains(
                            "(?i)syrup|suspension|solution"
                        )
                    )
                    .then(pl.lit("LIQUIDS"))
                    .when(
                        pl.col("product_description").str.contains(
                            "(?i)cream|ointment|gel"
                        )
                    )
                    .then(pl.lit("TOPICALS"))
                    .when(
                        pl.col("product_description").str.contains("(?i)drops|eye|ear")
                    )
                    .then(pl.lit("DROPS"))
                    .otherwise(pl.lit("OTHER"))
                    .alias("product_category")
                ]
            )

        # Business flags
        if "unit_price" in df.columns:
            df = df.with_columns(
                [(pl.col("unit_price") > 50).alias("is_high_value_item")]
            )

        if "quantity_sold" in df.columns:
            df = df.with_columns(
                [(pl.col("quantity_sold") > 20).alias("is_bulk_order")]
            )

        logger.info("✅ Calculated fields added")
        return df

    @staticmethod
    def add_calculated_fields_credit_note_header(df: pl.DataFrame) -> pl.DataFrame:
        """Add calculated fields for credit note headers"""

        logger.info("➕ Adding calculated fields for credit notes...")

        # Categorize credit notes
        if "credit_reason_code" in df.columns:
            df = df.with_columns(
                [
                    pl.when(
                        pl.col("credit_reason_code").str.contains("(?i)damage|defect")
                    )
                    .then(pl.lit("DAMAGED_PRODUCT"))
                    .when(
                        pl.col("credit_reason_code").str.contains("(?i)expire|expiry")
                    )
                    .then(pl.lit("EXPIRED_PRODUCT"))
                    .when(pl.col("credit_reason_code").str.contains("(?i)wrong|error"))
                    .then(pl.lit("WRONG_ITEM_SENT"))
                    .when(pl.col("credit_reason_code").str.contains("(?i)return"))
                    .then(pl.lit("CUSTOMER_RETURN"))
                    .when(
                        pl.col("credit_reason_code").str.contains("(?i)price|discount")
                    )
                    .then(pl.lit("PRICE_ADJUSTMENT"))
                    .otherwise(pl.lit("OTHER"))
                    .alias("credit_type")
                ]
            )

        # Refund percentage (will be calculated during reconciliation)
        if "refund_percentage" in df.columns:
            df = df.with_columns(
                [(pl.col("refund_percentage") >= 99).alias("is_full_refund")]
            )

        logger.info("✅ Calculated fields added")
        return df

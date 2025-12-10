# src/metadata_handler.py
"""
Metadata Handler for Bronze/Silver Metadata Management
"""
import polars as pl
from datetime import datetime
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class MetadataHandler:
    """Handle bronze and silver metadata"""

    def __init__(self, config: Dict):
        self.config = config
        self.bronze_keep = config["bronze_metadata"]["keep_columns"]
        self.bronze_rename = config["bronze_metadata"]["rename_mapping"]
        self.bronze_drop = config["bronze_metadata"]["drop_columns"]

    def optimize_bronze_metadata(self, df: pl.DataFrame) -> pl.DataFrame:
        """Optimize bronze metadata columns"""

        # Drop unnecessary columns
        existing_drop_cols = [col for col in self.bronze_drop if col in df.columns]
        if existing_drop_cols:
            df = df.drop(existing_drop_cols)
            logger.info(f"🗑️  Dropped {len(existing_drop_cols)} bronze metadata columns")

        # Rename bronze metadata
        rename_map = {k: v for k, v in self.bronze_rename.items() if k in df.columns}
        if rename_map:
            df = df.rename(rename_map)
            logger.info(f"✏️  Renamed {len(rename_map)} bronze metadata columns")
        else:
            logger.info("ℹ️  No bronze metadata columns to rename")

        return df

    def add_silver_metadata(
        self, df: pl.DataFrame, process_id: str, processing_start: datetime
    ) -> pl.DataFrame:
        """Add silver layer metadata"""

        processing_end = datetime.now()
        duration_ms = int((processing_end - processing_start).total_seconds() * 1000)

        df = df.with_columns(
            [
                pl.lit(processing_end).alias("_silver_processed_at"),
                pl.lit(process_id).alias("_silver_batch_id"),
                pl.lit(processing_start).alias("_silver_valid_from"),
                pl.lit(None).cast(pl.Datetime).alias("_silver_valid_to"),
                pl.lit(True).alias("_silver_is_current"),
                pl.lit(1).alias("_silver_record_version"),
                pl.lit(duration_ms).alias("_silver_processing_duration_ms"),
            ]
        )

        logger.info(f"✅ Added silver metadata (batch_id: {process_id})")
        return df

    def calculate_quality_score(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate data quality score for each row"""

        # Start with 100 points
        score = pl.lit(100.0)

        # Deduct for data quality flags
        if "amount_invalid_flag" in df.columns:
            score = score - (pl.col("amount_invalid_flag") * 25)

        if "date_invalid_flag" in df.columns:
            score = score - (pl.col("date_invalid_flag") * 20)

        if "customer_missing_flag" in df.columns:
            score = score - (pl.col("customer_missing_flag") * 15)

        if "quantity_invalid_flag" in df.columns:
            score = score - (pl.col("quantity_invalid_flag") * 10)
        if "price_invalid_flag" in df.columns:
            score = score - (pl.col("price_invalid_flag") * 10)

        if "has_amount_variance" in df.columns:
            score = score - (pl.col("has_amount_variance").cast(pl.Int32) * 10)

        # Ensure score is between 0 and 100
        score = pl.when(score < 0).then(0).when(score > 100).then(100).otherwise(score)

        df = df.with_columns(score.alias("_silver_data_quality_score"))

        avg_score = df.select(pl.col("_silver_data_quality_score").mean()).item()
        logger.info(f"📊 Average data quality score: {avg_score:.2f}/100")

        return df

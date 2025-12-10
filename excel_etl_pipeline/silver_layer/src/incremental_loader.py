# src/incremental_loader.py
"""
Incremental Loading Manager with Checkpoint Support
"""
import polars as pl
from deltalake import DeltaTable
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict
import json
import logging

logger = logging.getLogger(__name__)


class IncrementalLoader:
    """Manage incremental loading with checkpoints"""

    def __init__(self, checkpoint_dir: Path):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def get_checkpoint(self, table_name: str) -> Optional[Dict]:
        """Get last checkpoint for a table"""
        checkpoint_file = self.checkpoint_dir / f"{table_name}_checkpoint.json"

        if checkpoint_file.exists():
            with open(checkpoint_file, "r") as f:
                return json.load(f)
        return None

    def read_incremental(
        self,
        delta_path: Path,
        table_name: str,
        timestamp_column: str = "_raw_processed_at",
    ) -> pl.DataFrame:
        """Read only new records since last checkpoint"""

        checkpoint = self.get_checkpoint(table_name)

        try:
            dt = DeltaTable(str(delta_path))

            if checkpoint and checkpoint.get("last_processed_timestamp"):
                last_processed = checkpoint["last_processed_timestamp"]
                logger.info(
                    f"📥 Incremental load: Reading records after {last_processed}"
                )

                # Read with filter
                df = pl.from_arrow(dt.to_pyarrow_table())
                df = df.filter(pl.col(timestamp_column) > last_processed)

                logger.info(f"✅ Loaded {len(df):,} NEW records")
            else:
                # First load - get all records
                logger.info(f"📥 Initial load: Reading ALL records")
                df = pl.from_arrow(dt.to_pyarrow_table())
                logger.info(f"✅ Loaded {len(df):,} records (initial load)")

            return df

        except Exception as e:
            logger.error(f"❌ Failed to read from {delta_path}: {e}")
            return pl.DataFrame()

    def update_checkpoint(
        self, table_name: str, max_timestamp: datetime, records_processed: int
    ):
        """Update checkpoint after successful processing"""
        checkpoint_file = self.checkpoint_dir / f"{table_name}_checkpoint.json"

        checkpoint = {
            "table_name": table_name,
            "last_processed_timestamp": max_timestamp.isoformat(),
            "checkpoint_updated_at": datetime.now().isoformat(),
            "records_processed": records_processed,
            "status": "SUCCESS",
        }

        with open(checkpoint_file, "w") as f:
            json.dump(checkpoint, f, indent=2)

        logger.info(f"✅ Checkpoint updated: {table_name} -> {max_timestamp}")

    def reset_checkpoint(self, table_name: str):
        """Reset checkpoint to force full reload"""
        checkpoint_file = self.checkpoint_dir / f"{table_name}_checkpoint.json"
        if checkpoint_file.exists():
            checkpoint_file.unlink()
            logger.info(f"🔄 Checkpoint reset: {table_name}")

"""Parquet file operations."""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from .config import Config


class ParquetManager:
    """Handles parquet file operations."""

    def __init__(self, logger):
        self.logger = logger

    def save_parquet(self, df, filepath):
        """Save DataFrame to parquet with compression."""
        table = pa.Table.from_pandas(df)

        pq.write_table(
            table,
            filepath,
            compression="zstd",
            compression_level=Config.COMPRESSION_LEVEL,
            use_dictionary=True,
            write_statistics=True,
            version="2.6",
            data_page_size=Config.DATA_PAGE_SIZE,
            # Removed dictionary_page_size_limit parameter as it's causing issues
            # Can be added back later with correct parameter name if needed
        )

    def update_dataset(self, new_df, date_str):
        """Update main dataset - handles duplicates by date."""
        self.logger.info("💾 Updating parquet dataset...")

        try:
            parquet_path = Path(Config.PARQUET_FILE)

            if parquet_path.exists():
                # Load existing data
                existing_df = pd.read_parquet(parquet_path)

                # Remove any existing data for this date (handles duplicates)
                existing_df = existing_df[
                    existing_df["crawl_date"].dt.strftime("%Y-%m-%d") != date_str
                ]

                # Combine and sort
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                combined_df = combined_df.sort_values(
                    ["year", "month", "day", "personName"]
                )

                self.save_parquet(combined_df, parquet_path)

                # Log results
                file_size_mb = parquet_path.stat().st_size / (1024 * 1024)
                self.logger.info(
                    f"✅ Updated dataset: {len(combined_df):,} total records"
                )
                self.logger.info(f"📦 File size: {file_size_mb:.2f} MB")
            else:
                # Create new file
                # Ensure parent directory exists
                parquet_path.parent.mkdir(parents=True, exist_ok=True)
                self.save_parquet(new_df, parquet_path)
                self.logger.info("✅ Created initial parquet dataset")

            return True

        except Exception as e:
            self.logger.error(f"❌ Failed to update dataset: {e}")
            return False

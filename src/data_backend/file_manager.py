"""Parquet file operations."""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

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
            dictionary_pagesize_limit=Config.DICT_PAGE_SIZE,
        )

    def update_dataset(self, new_df, date_str):
        """Update main dataset - handles duplicates by date."""
        self.logger.info("💾 Updating parquet dataset...")

        try:
            if os.path.exists(Config.PARQUET_FILE):
                # Load existing data
                existing_df = pd.read_parquet(Config.PARQUET_FILE)

                # Remove any existing data for this date (handles duplicates)
                existing_df = existing_df[
                    existing_df["crawl_date"].dt.strftime("%Y-%m-%d") != date_str
                ]

                # Combine and sort
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                combined_df = combined_df.sort_values(
                    ["year", "month", "day", "personName"]
                )

                self.save_parquet(combined_df, Config.PARQUET_FILE)

                # Log results
                file_size_mb = os.path.getsize(Config.PARQUET_FILE) / (1024 * 1024)
                self.logger.info(
                    f"✅ Updated dataset: {len(combined_df):,} total records"
                )
                self.logger.info(f"📦 File size: {file_size_mb:.2f} MB")
            else:
                # Create new file
                self.save_parquet(new_df, Config.PARQUET_FILE)
                self.logger.info("✅ Created initial parquet dataset")

            return True

        except Exception as e:
            self.logger.error(f"❌ Failed to update dataset: {e}")
            return False

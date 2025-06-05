#!/usr/bin/env python3
"""
RedFlagProfits Historical Data Recovery - Optimized Version

Recovers missing historical data from the Wayback Machine and integrates it
into the existing dataset using batched saves and optional async processing.
"""

import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
import json
from io import StringIO
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Tuple

from data_backend import Config, DataProcessor, ParquetManager
from data_backend.utils import retry_on_network_error


class OptimizedWaybackRecoveryClient:
    """Handles Wayback Machine data recovery operations with async support."""

    def __init__(self, logger, batch_size=20):
        self.logger = logger
        self.batch_size = batch_size
        self.session = requests.Session()
        self.session.headers.update(Config.HEADERS)

        # Wayback Machine endpoints
        self.cdx_api = "https://web.archive.org/cdx/search/cdx"
        self.wayback_base = "https://web.archive.org/web"

        # Original Forbes API endpoints to try
        self.forbes_endpoints = [
            "https://www.forbes.com/forbesapi/person/rtb/0/position/true.json",
            "https://www.forbes.com/forbesapi/person/rtb/0/-estWorthPrev/true.json",
            "https://www.forbes.com/forbesapi/person/rtb/0/.json",
        ]

    def get_available_snapshots(self, start_date="2020-01-01", end_date=None):
        """Get all available snapshots from the Wayback Machine."""
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        all_snapshots = []

        for endpoint in self.forbes_endpoints:
            self.logger.info(f"🔍 Searching for snapshots of {endpoint}")
            snapshots = self._query_cdx_api(endpoint, start_date, end_date)
            if snapshots:
                self.logger.info(
                    f"✅ Found {len(snapshots)} snapshots for this endpoint"
                )
                all_snapshots.extend(snapshots)
            else:
                self.logger.warning(f"⚠️  No snapshots found for {endpoint}")

        # Remove duplicates and sort by timestamp
        unique_snapshots = self._deduplicate_snapshots(all_snapshots)
        unique_snapshots.sort(key=lambda x: x["timestamp"])

        self.logger.info(f"📊 Total unique snapshots found: {len(unique_snapshots)}")
        return unique_snapshots

    def _query_cdx_api(self, url, start_date, end_date):
        """Query the CDX API for available snapshots."""
        params = {
            "url": url,
            "output": "json",
            "from": start_date.replace("-", ""),
            "to": end_date.replace("-", ""),
            "filter": ["statuscode:200", "mimetype:application/json"],
            "collapse": "timestamp:8",  # Collapse to daily snapshots
        }

        try:
            response = self.session.get(self.cdx_api, params=params, timeout=300)
            response.raise_for_status()

            data = response.json()
            if not data:
                return []

            # First row is headers, rest are data
            headers = data[0]
            snapshots = []

            for row in data[1:]:
                snapshot = dict(zip(headers, row))
                # Parse timestamp to datetime
                try:
                    dt = datetime.strptime(snapshot["timestamp"], "%Y%m%d%H%M%S")
                    snapshot["datetime"] = dt
                    snapshot["date"] = dt.strftime("%Y-%m-%d")
                    snapshots.append(snapshot)
                except ValueError:
                    continue

            return snapshots

        except Exception as e:
            self.logger.error(f"❌ CDX API query failed for {url}: {e}")
            return []

    def _deduplicate_snapshots(self, snapshots):
        """Remove duplicate snapshots, keeping the best one per day."""
        daily_snapshots = {}

        for snapshot in snapshots:
            date = snapshot["date"]
            if date not in daily_snapshots:
                daily_snapshots[date] = snapshot
            else:
                # Keep the one with higher status code or later in day
                existing = daily_snapshots[date]
                if (
                    int(snapshot.get("statuscode", 0))
                    > int(existing.get("statuscode", 0))
                    or snapshot["timestamp"] > existing["timestamp"]
                ):
                    daily_snapshots[date] = snapshot

        return list(daily_snapshots.values())

    @retry_on_network_error(logger=None, operation_name="Wayback Machine fetch")
    def fetch_archived_data(self, snapshot):
        """Fetch and process data from a specific Wayback Machine snapshot."""
        wayback_url = (
            f"{self.wayback_base}/{snapshot['timestamp']}id_/{snapshot['original']}"
        )
        try:
            self.logger.debug(
                f"📥 Fetching: {snapshot['date']} ({snapshot['timestamp']})"
            )

            response = self.session.get(wayback_url, timeout=30)
            response.raise_for_status()

            # Parse the JSON response
            raw_data = pd.read_json(StringIO(response.text))
            data = pd.json_normalize(raw_data["personList"]["personsLists"])

            # Use the snapshot date as crawl_date
            clean_data = data[Config.FORBES_COLUMNS].copy()
            clean_data["crawl_date"] = pd.to_datetime(snapshot["date"])

            self.logger.debug(
                f"✅ Processed {len(clean_data)} records for {snapshot['date']}"
            )
            return clean_data, snapshot["date"]

        except Exception as e:
            self.logger.error(f"❌ Failed to fetch {snapshot['date']}: {e}")
            return None

    async def fetch_archived_data_async(self, session, snapshot):
        """Async version of fetch_archived_data for better concurrency."""
        wayback_url = (
            f"{self.wayback_base}/{snapshot['timestamp']}id_/{snapshot['original']}"
        )
        try:
            self.logger.debug(
                f"📥 Fetching async: {snapshot['date']} ({snapshot['timestamp']})"
            )

            async with session.get(wayback_url, timeout=30) as response:
                response.raise_for_status()
                text = await response.text()

            # Parse the JSON response
            raw_data = pd.read_json(StringIO(text))
            data = pd.json_normalize(raw_data["personList"]["personsLists"])

            # Use the snapshot date as crawl_date
            clean_data = data[Config.FORBES_COLUMNS].copy()
            clean_data["crawl_date"] = pd.to_datetime(snapshot["date"])

            self.logger.debug(
                f"✅ Processed {len(clean_data)} records for {snapshot['date']}"
            )
            return clean_data, snapshot["date"]

        except Exception as e:
            self.logger.error(f"❌ Failed to fetch async {snapshot['date']}: {e}")
            return None


class OptimizedHistoricalDataRecovery:
    """Main recovery orchestration class with batched saves and async support."""

    def __init__(self, batch_size=20, use_async=False):
        self.logger = self._setup_logging()
        self.batch_size = batch_size
        self.use_async = use_async
        self.wayback_client = OptimizedWaybackRecoveryClient(self.logger, batch_size)
        self.processor = DataProcessor(self.logger)
        self.file_manager = ParquetManager(self.logger)

        # Batch storage
        self.pending_data = []
        self.pending_dates = []

    def _setup_logging(self):
        """Setup logging for recovery operations."""
        log_path = Path("recovery.log")
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_path), logging.StreamHandler()],
        )
        return logging.getLogger(__name__)

    def get_existing_dates(self):
        """Get dates that already exist in the current dataset."""
        try:
            if Config.PARQUET_FILE.exists():
                existing_data = pd.read_parquet(Config.PARQUET_FILE)
                existing_dates = set(
                    existing_data["crawl_date"].dt.strftime("%Y-%m-%d")
                )
                self.logger.info(
                    f"📊 Found {len(existing_dates)} existing dates in dataset"
                )
                return existing_dates
            else:
                self.logger.info("📊 No existing dataset found - will recover all data")
                return set()
        except Exception as e:
            self.logger.error(f"❌ Failed to read existing data: {e}")
            return set()

    def _save_batch(self, force_save=False):
        """Save accumulated batch data to dataset."""
        if not self.pending_data or (
            len(self.pending_data) < self.batch_size and not force_save
        ):
            return True

        self.logger.info(f"💾 Saving batch of {len(self.pending_data)} records...")

        try:
            # Combine all pending data
            combined_data = pd.concat(self.pending_data, ignore_index=True)

            # Use a single date string for the batch (could be improved)
            batch_date_str = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            # Process combined data
            processed_data = self.processor.process_data(combined_data)
            processed_data = self.processor.add_inflation_data(
                processed_data, None, None
            )

            # Save to dataset
            success = self.file_manager.update_dataset(processed_data, batch_date_str)

            if success:
                self.logger.info(
                    f"✅ Successfully saved batch for dates: {', '.join(self.pending_dates)}"
                )
                # Clear the batch
                self.pending_data.clear()
                self.pending_dates.clear()
                return True
            else:
                self.logger.error(f"❌ Failed to save batch")
                return False

        except Exception as e:
            self.logger.error(f"❌ Batch save failed: {e}")
            return False

    def _process_single_result(self, result):
        """Process a single fetch result and add to batch."""
        if result is None:
            return False

        forbes_data, date_str = result

        # Add to pending batch
        self.pending_data.append(forbes_data)
        self.pending_dates.append(date_str)

        self.logger.info(
            f"📦 Added {date_str} to batch ({len(self.pending_data)}/{self.batch_size})"
        )

        # Save if batch is full
        if len(self.pending_data) >= self.batch_size:
            return self._save_batch()

        return True

    async def _recover_batch_async(self, snapshots_batch):
        """Process a batch of snapshots asynchronously."""
        async with aiohttp.ClientSession(
            headers=Config.HEADERS, timeout=aiohttp.ClientTimeout(total=60)
        ) as session:
            tasks = [
                self.wayback_client.fetch_archived_data_async(session, snapshot)
                for snapshot in snapshots_batch
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            successful_results = []
            for result in results:
                if isinstance(result, Exception):
                    self.logger.error(f"❌ Async fetch failed: {result}")
                elif result is not None:
                    successful_results.append(result)

            return successful_results

    def recover_historical_data(
        self,
        start_date="2020-01-01",
        end_date=None,
        dry_run=False,
    ):
        """Main recovery process with batched saves."""
        self.logger.info(
            "🚀 Starting optimized historical data recovery from Wayback Machine"
        )
        self.logger.info(
            f"⚙️  Batch size: {self.batch_size}, Async mode: {self.use_async}"
        )

        # Get existing dates to avoid duplicates
        existing_dates = self.get_existing_dates()

        # Get available snapshots
        snapshots = self.wayback_client.get_available_snapshots(start_date, end_date)

        if not snapshots:
            self.logger.error("❌ No snapshots found to recover")
            return False

        # Filter out dates we already have
        new_snapshots = [s for s in snapshots if s["date"] not in existing_dates]

        self.logger.info(f"📈 Found {len(new_snapshots)} new dates to recover")
        self.logger.info(
            f"⏭️  Skipping {len(snapshots) - len(new_snapshots)} existing dates"
        )

        if not new_snapshots:
            self.logger.info("✅ All available data already recovered")
            return True

        if dry_run:
            self.logger.info("🔍 DRY RUN - Would recover the following dates:")
            for snapshot in new_snapshots[:10]:  # Show first 10
                self.logger.info(f"  📅 {snapshot['date']} ({snapshot['timestamp']})")
            if len(new_snapshots) > 10:
                self.logger.info(f"  ... and {len(new_snapshots) - 10} more")
            return True

        if self.use_async:
            return asyncio.run(self._recover_async(new_snapshots))
        else:
            return self._recover_sync(new_snapshots)

    async def _recover_async(self, snapshots):
        """Async recovery process."""
        successful_recoveries = 0
        failed_recoveries = 0

        # Process in smaller concurrent batches to avoid overwhelming servers
        concurrent_batch_size = 5

        for i in range(0, len(snapshots), concurrent_batch_size):
            batch = snapshots[i : i + concurrent_batch_size]
            self.logger.info(
                f"🔄 Processing async batch {i//concurrent_batch_size + 1}"
            )

            results = await self._recover_batch_async(batch)

            for result in results:
                if self._process_single_result(result):
                    successful_recoveries += 1
                else:
                    failed_recoveries += 1

            # Brief pause between concurrent batches
            await asyncio.sleep(1)

        # Save any remaining data
        self._save_batch(force_save=True)

        # Save updated dictionaries
        self.processor.save_dictionaries()

        return self._log_summary(successful_recoveries, failed_recoveries)

    def _recover_sync(self, snapshots):
        """Synchronous recovery process with batched saves."""
        successful_recoveries = 0
        failed_recoveries = 0

        for i, snapshot in enumerate(snapshots, 1):
            self.logger.info(f"📊 Processing {i}/{len(snapshots)}: {snapshot['date']}")

            # Fetch archived data
            result = self.wayback_client.fetch_archived_data(snapshot)

            if self._process_single_result(result):
                successful_recoveries += 1
            else:
                failed_recoveries += 1

            # Rate limiting - be nice to Wayback Machine
            if i % 10 == 0:
                self.logger.info("⏸️  Brief pause to avoid overwhelming servers...")
                time.sleep(2)
            else:
                time.sleep(0.5)

        # Save any remaining data in the final batch
        self._save_batch(force_save=True)

        # Save updated dictionaries
        self.processor.save_dictionaries()

        return self._log_summary(successful_recoveries, failed_recoveries)

    def _log_summary(self, successful_recoveries, failed_recoveries):
        """Log recovery summary and return success status."""
        self.logger.info(f"🎉 Recovery completed!")
        self.logger.info(f"✅ Successful recoveries: {successful_recoveries}")
        self.logger.info(f"❌ Failed recoveries: {failed_recoveries}")
        if successful_recoveries + failed_recoveries > 0:
            self.logger.info(
                f"📊 Success rate: {successful_recoveries/(successful_recoveries+failed_recoveries)*100:.1f}%"
            )

        return successful_recoveries > 0


def main():
    """Main execution function."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Recover historical billionaire data from Wayback Machine (Optimized)"
    )
    parser.add_argument(
        "--start-date",
        default="2020-01-01",
        help="Start date for recovery (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date", default=None, help="End date for recovery (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be recovered without actually doing it",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="Number of records to process before saving (default: 20)",
    )
    parser.add_argument(
        "--async",
        action="store_true",
        dest="use_async",
        help="Use async processing for faster downloads",
    )

    args = parser.parse_args()

    # Ensure data directory exists
    Config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    Config.DICT_DIR.mkdir(parents=True, exist_ok=True)

    recovery = OptimizedHistoricalDataRecovery(
        batch_size=args.batch_size, use_async=args.use_async
    )
    success = recovery.recover_historical_data(
        start_date=args.start_date, end_date=args.end_date, dry_run=args.dry_run
    )

    exit(0 if success else 1)


if __name__ == "__main__":
    main()

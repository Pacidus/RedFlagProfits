#!/usr/bin/env python3
"""
RedFlagProfits Historical Data Recovery

Recovers missing historical data from the Wayback Machine and integrates it
into the existing dataset using the established data pipeline.
"""

import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
import json
from io import StringIO

from data_backend import Config, DataProcessor, ParquetManager
from data_backend.utils import retry_on_network_error


class WaybackRecoveryClient:
    """Handles Wayback Machine data recovery operations."""

    def __init__(self, logger):
        self.logger = logger
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
            self.logger.info(
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

            self.logger.info(
                f"✅ Processed {len(clean_data)} records for {snapshot['date']}"
            )
            return clean_data, snapshot["date"]

        except Exception as e:
            self.logger.error(f"❌ Failed to fetch {snapshot['date']}: {e}")
            return None


class HistoricalDataRecovery:
    """Main recovery orchestration class."""

    def __init__(self):
        self.logger = self._setup_logging()
        self.wayback_client = WaybackRecoveryClient(self.logger)
        self.processor = DataProcessor(self.logger)
        self.file_manager = ParquetManager(self.logger)

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

    def recover_historical_data(
        self,
        start_date="2020-01-01",
        end_date=None,
        dry_run=False,
    ):
        """Main recovery process."""
        self.logger.info("🚀 Starting historical data recovery from Wayback Machine")

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

        # Process snapshots in batches
        successful_recoveries = 0
        failed_recoveries = 0

        for i, snapshot in enumerate(new_snapshots, 1):
            self.logger.info(
                f"📊 Processing {i}/{len(new_snapshots)}: {snapshot['date']}"
            )

            # Fetch archived data
            result = self.wayback_client.fetch_archived_data(snapshot)
            if result is None:
                failed_recoveries += 1
                continue

            forbes_data, date_str = result

            # Process using existing pipeline
            try:
                processed_data = self.processor.process_data(forbes_data)
                # Add empty inflation data (historical inflation can be added later if needed)
                processed_data = self.processor.add_inflation_data(
                    processed_data, None, None
                )

                # Save to dataset
                success = self.file_manager.update_dataset(processed_data, date_str)
                if success:
                    successful_recoveries += 1
                    self.logger.info(f"✅ Successfully recovered {date_str}")
                else:
                    failed_recoveries += 1
                    self.logger.error(f"❌ Failed to save {date_str}")

            except Exception as e:
                failed_recoveries += 1
                self.logger.error(f"❌ Processing failed for {date_str}: {e}")

            # Rate limiting - be nice to Wayback Machine
            if i % 10 == 0:
                self.logger.info("⏸️  Brief pause to avoid overwhelming servers...")
                time.sleep(2)
            else:
                time.sleep(0.5)

        # Save updated dictionaries
        self.processor.save_dictionaries()

        # Summary
        self.logger.info(f"🎉 Recovery completed!")
        self.logger.info(f"✅ Successful recoveries: {successful_recoveries}")
        self.logger.info(f"❌ Failed recoveries: {failed_recoveries}")
        self.logger.info(
            f"📊 Success rate: {successful_recoveries/(successful_recoveries+failed_recoveries)*100:.1f}%"
        )

        return successful_recoveries > 0


def main():
    """Main execution function."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Recover historical billionaire data from Wayback Machine"
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

    args = parser.parse_args()

    # Ensure data directory exists
    Config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    Config.DICT_DIR.mkdir(parents=True, exist_ok=True)

    recovery = HistoricalDataRecovery()
    success = recovery.recover_historical_data(
        start_date=args.start_date, end_date=args.end_date, dry_run=args.dry_run
    )

    exit(0 if success else 1)


if __name__ == "__main__":
    main()

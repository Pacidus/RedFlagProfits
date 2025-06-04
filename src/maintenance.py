#!/usr/bin/env python3
"""
RedFlagProfits Standalone Data Maintenance Script

Handles inflation data updates and duplicate record merging for historical data.
Uses existing data_backend library without requiring any modifications.
FIXED VERSION - resolves pandas boolean ambiguity issues.
"""

import logging
import pandas as pd
import numpy as np
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

# Add src to path to import existing data_backend
sys.path.insert(0, str(Path(__file__).parent / "src"))

from data_backend import Config, FredClient
from data_backend.utils import safe_numeric_conversion


class StandaloneDataMaintenance:
    """Standalone data maintenance using existing data_backend library."""

    def __init__(self, logger):
        self.logger = logger
        self.fred_client = FredClient(logger)

    def _is_null_safe(self, value):
        """Safely check if a value is null without triggering pandas ambiguity."""
        if value is None:
            return True

        try:
            # For scalar values, use pandas isna
            if not hasattr(value, "__len__") or isinstance(value, str):
                return pd.isna(value)

            # For array-like values, check if empty or all null
            if hasattr(value, "__len__"):
                if len(value) == 0:
                    return True
                # For lists, check if all elements are null
                if isinstance(value, list):
                    return all(
                        x is None
                        or (hasattr(x, "__len__") and len(str(x).strip()) == 0)
                        for x in value
                    )

            return False
        except (ValueError, TypeError):
            # If any check fails, assume it's not null
            return False

    def run_full_maintenance(self, parquet_path, operations=None):
        """Run complete data maintenance operations."""
        if operations is None:
            operations = ["duplicates", "inflation", "analysis"]

        self.logger.info("🔧 Starting standalone data maintenance...")

        if not Path(parquet_path).exists():
            self.logger.error(f"❌ Parquet file not found: {parquet_path}")
            return False

        try:
            # Load data
            self.logger.info("📂 Loading dataset...")
            df = pd.read_parquet(parquet_path)
            original_count = len(df)
            self.logger.info(f"📊 Loaded {original_count:,} records")

            # Initial analysis
            if "analysis" in operations:
                initial_quality = self._analyze_data_quality(df, "Initial")

            # Handle duplicates
            if "duplicates" in operations:
                df = self._detect_and_merge_duplicates(df)

            # Update inflation data
            if "inflation" in operations:
                df = self._update_missing_inflation_data(df)

            # Final analysis
            if "analysis" in operations:
                final_quality = self._analyze_data_quality(df, "Final")
                self._report_improvements(
                    original_count, len(df), initial_quality, final_quality
                )

            # Save updated data
            self.logger.info("💾 Saving updated dataset...")
            df.to_parquet(
                parquet_path,
                compression="zstd",
                compression_level=Config.COMPRESSION_LEVEL,
            )

            file_size_mb = Path(parquet_path).stat().st_size / (1024 * 1024)
            self.logger.info(f"📦 Updated file size: {file_size_mb:.2f} MB")

            self.logger.info("✅ Data maintenance completed successfully!")
            return True

        except Exception as e:
            self.logger.error(f"❌ Data maintenance failed: {e}")
            self.logger.exception("Full traceback:")
            return False

    def _detect_and_merge_duplicates(self, df):
        """Detect and intelligently merge duplicate records."""
        self.logger.info("🔍 Detecting duplicate records...")

        # Group by date and person to find duplicates
        duplicate_groups = df.groupby(["crawl_date", "personName"]).size()
        duplicates = duplicate_groups[duplicate_groups > 1]

        if len(duplicates) == 0:
            self.logger.info("✅ No duplicate records found")
            return df

        total_duplicates = duplicates.sum() - len(duplicates)
        self.logger.info(
            f"⚠️ Found {len(duplicates)} duplicate person-date combinations"
        )
        self.logger.info(f"📊 Total duplicate records to merge: {total_duplicates}")

        # Process each duplicate group
        merged_records = []
        processed_groups = set()

        for (date, person), count in duplicates.items():
            group_key = (date, person)
            if group_key in processed_groups:
                continue

            # Get all records for this person-date combination
            mask = (df["crawl_date"] == date) & (df["personName"] == person)
            group_records = df[mask].copy()

            # Merge the group intelligently
            try:
                merged_record = self._merge_duplicate_group(group_records, date, person)
                merged_records.append(merged_record)
                processed_groups.add(group_key)
            except Exception as e:
                self.logger.warning(
                    f"⚠️ Failed to merge group for {person} on {date}: {e}"
                )
                # Keep the first record as fallback
                merged_records.append(group_records.iloc[0].to_dict())
                processed_groups.add(group_key)

        if len(merged_records) == 0:
            return df

        # Remove original duplicates and add merged records
        for (date, person), _ in duplicates.items():
            mask = (df["crawl_date"] == date) & (df["personName"] == person)
            df = df[~mask]

        # Add merged records
        merged_df = pd.DataFrame(merged_records)
        result_df = pd.concat([df, merged_df], ignore_index=True)

        # Sort by date and name for consistency
        result_df = result_df.sort_values(["crawl_date", "personName"]).reset_index(
            drop=True
        )

        removed_count = total_duplicates
        self.logger.info(
            f"✅ Merged duplicates: {len(df) + total_duplicates:,} → {len(result_df):,} records"
        )
        self.logger.info(f"🗑️ Removed {removed_count} duplicate records")

        return result_df

    def _merge_duplicate_group(self, group_records, date, person):
        """Intelligently merge duplicate records for the same person-date."""
        if len(group_records) == 1:
            return group_records.iloc[0].to_dict()

        self.logger.debug(
            f"🔀 Merging {len(group_records)} records for {person} on {date}"
        )

        # Start with the first record as base
        merged = group_records.iloc[0].copy()

        # Smart merging rules for wealth columns (take maximum non-null value)
        wealth_columns = [
            "finalWorth",
            "estWorthPrev",
            "privateAssetsWorth",
            "archivedWorth",
        ]
        for col in wealth_columns:
            if col in group_records.columns:
                valid_values = group_records[col].dropna()
                if len(valid_values) > 0:
                    merged[col] = valid_values.max()

        # For categorical fields, prefer non-null/non-empty values
        categorical_columns = [
            "gender",
            "countryOfCitizenship",
            "state",
            "city",
            "source",
        ]
        for col in categorical_columns:
            if col in group_records.columns:
                valid_values = group_records[col].dropna()

                # Safely remove empty strings
                if len(valid_values) > 0:
                    try:
                        # Check if it's a string column and filter empty strings
                        if valid_values.dtype == "object":
                            # Convert to string and filter
                            str_values = valid_values.astype(str)
                            non_empty_indices = str_values.str.strip() != ""
                            if non_empty_indices.any():
                                valid_values = valid_values[non_empty_indices]
                    except (ValueError, AttributeError, TypeError):
                        # If string operations fail, continue with current values
                        pass

                    if len(valid_values) > 0:
                        # Take the most common value, or first if tied
                        try:
                            mode_values = valid_values.mode()
                            if len(mode_values) > 0:
                                merged[col] = mode_values.iloc[0]
                            else:
                                merged[col] = valid_values.iloc[0]
                        except (ValueError, AttributeError, TypeError):
                            # Fallback: just take first value
                            merged[col] = valid_values.iloc[0]

        # For encoded fields, take maximum code (usually means more complete data)
        code_columns = ["country_code", "source_code"]
        for col in code_columns:
            if col in group_records.columns:
                valid_values = group_records[col].dropna()
                if len(valid_values) > 0:
                    try:
                        # Safely filter out invalid codes
                        filtered_values = []
                        for val in valid_values:
                            try:
                                if val != Config.INVALID_CODE:
                                    filtered_values.append(val)
                            except (ValueError, TypeError):
                                filtered_values.append(val)

                        if filtered_values:
                            merged[col] = max(filtered_values)
                        else:
                            merged[col] = valid_values.iloc[0]
                    except (ValueError, TypeError):
                        # Fallback: just take the first valid value
                        merged[col] = valid_values.iloc[0]

        # For date fields, take the most recent non-null value
        if "birthDate" in group_records.columns:
            valid_dates = group_records["birthDate"].dropna()
            if len(valid_dates) > 0:
                merged["birthDate"] = valid_dates.iloc[-1]

        # For list/array columns, merge and deduplicate - ULTRA SAFE VERSION
        list_columns = ["industry_codes"]
        asset_columns = [f"asset_{col}" for col in Config.ASSET_COLUMNS]
        list_columns.extend(asset_columns)

        for col in list_columns:
            if col in group_records.columns:
                all_values = []
                for idx, record in group_records.iterrows():
                    try:
                        value = record[col]

                        # Ultra-simple check: only process if it's clearly a list
                        if isinstance(value, list) and len(value) > 0:
                            all_values.extend(value)

                    except (ValueError, TypeError, AttributeError):
                        # Skip any problematic values
                        continue

                # Remove duplicates while preserving order
                merged[col] = list(dict.fromkeys(all_values)) if all_values else []

        # Keep inflation data (should be the same for same date)
        inflation_cols = ["cpi_u", "pce"]
        for col in inflation_cols:
            if col in group_records.columns:
                valid_values = group_records[col].dropna()
                if len(valid_values) > 0:
                    merged[col] = valid_values.iloc[0]

        return merged.to_dict()

    def _update_missing_inflation_data(self, df):
        """Update missing inflation data for all dates in the dataset."""
        self.logger.info("🔄 Checking for missing inflation data...")

        # Safely check for missing inflation data
        try:
            missing_cpi = df["cpi_u"].isna()
            missing_pce = df["pce"].isna()
            missing_any = missing_cpi | missing_pce

            if not missing_any.any():
                self.logger.info("✅ All inflation data is already present")
                return df
        except KeyError:
            self.logger.warning("⚠️ Inflation columns not found, will add them")
            missing_any = pd.Series([True] * len(df))

        # Get unique dates that need inflation data
        if missing_any.any():
            dates_needing_update = df[missing_any]["crawl_date"].dt.date.unique()
            self.logger.info(
                f"📅 Found {len(dates_needing_update)} dates needing inflation data"
            )

            # Batch fetch inflation data
            inflation_data = self._batch_fetch_inflation_data(dates_needing_update)

            if not inflation_data:
                self.logger.warning("⚠️ No inflation data could be fetched")
                return df

            # Apply updates
            updated_df = self._apply_inflation_updates(df, inflation_data)

            self.logger.info(
                f"✅ Updated inflation data for {len(inflation_data)} dates"
            )
            return updated_df

        return df

    def _batch_fetch_inflation_data(self, dates):
        """Batch fetch inflation data for multiple dates."""
        if not self.fred_client.api_key:
            self.logger.warning("⚠️ No FRED API key - skipping inflation data update")
            return {}

        # Determine date range for batch fetch
        min_date = min(dates)
        max_date = max(dates)

        # Add buffer for data availability
        start_date = min_date - timedelta(days=Config.INFLATION_BUFFER_DAYS)
        end_date = max_date + timedelta(days=30)

        self.logger.info(f"📡 Fetching inflation data from {start_date} to {end_date}")

        # Fetch both series using existing client methods
        date_range = (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        cpi_data = self.fred_client._fetch_series(Config.CPI_SERIES, *date_range)
        pce_data = self.fred_client._fetch_series(Config.PCE_SERIES, *date_range)

        if cpi_data is None or pce_data is None:
            self.logger.warning("⚠️ Failed to fetch inflation series data")
            return {}

        # Process data into lookup dictionary
        inflation_lookup = {}

        for target_date in dates:
            target_month = pd.to_datetime(target_date).to_period("M")

            cpi_value = self.fred_client._get_monthly_value(
                cpi_data, target_month, "CPI-U"
            )
            pce_value = self.fred_client._get_monthly_value(
                pce_data, target_month, "PCE"
            )

            if cpi_value is not None and pce_value is not None:
                inflation_lookup[target_date] = {"cpi_u": cpi_value, "pce": pce_value}

        self.logger.info(
            f"📊 Successfully fetched inflation data for {len(inflation_lookup)} dates"
        )
        return inflation_lookup

    def _apply_inflation_updates(self, df, inflation_data):
        """Apply inflation data updates to the dataframe."""
        df_copy = df.copy()

        # Ensure inflation columns exist
        if "cpi_u" not in df_copy.columns:
            df_copy["cpi_u"] = np.nan
        if "pce" not in df_copy.columns:
            df_copy["pce"] = np.nan

        for date, values in inflation_data.items():
            mask = df_copy["crawl_date"].dt.date == date

            # Update missing CPI-U values
            try:
                cpi_missing = df_copy.loc[mask, "cpi_u"].isna()
                if cpi_missing.any():
                    df_copy.loc[mask & cpi_missing, "cpi_u"] = values["cpi_u"]
            except KeyError:
                df_copy.loc[mask, "cpi_u"] = values["cpi_u"]

            # Update missing PCE values
            try:
                pce_missing = df_copy.loc[mask, "pce"].isna()
                if pce_missing.any():
                    df_copy.loc[mask & pce_missing, "pce"] = values["pce"]
            except KeyError:
                df_copy.loc[mask, "pce"] = values["pce"]

        return df_copy

    def _analyze_data_quality(self, df, stage=""):
        """Analyze and report data quality metrics."""
        stage_prefix = f"{stage} " if stage else ""
        self.logger.info(f"📊 {stage_prefix}data quality analysis...")

        total_records = len(df)
        unique_dates = df["crawl_date"].nunique()
        unique_people = df["personName"].nunique()
        date_range = (
            f"{df['crawl_date'].min():%Y-%m-%d} to {df['crawl_date'].max():%Y-%m-%d}"
        )

        # Missing data analysis
        missing_analysis = {}
        critical_columns = ["finalWorth", "personName", "crawl_date"]

        for col in critical_columns:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                missing_pct = (missing_count / total_records) * 100
                missing_analysis[col] = {
                    "count": missing_count,
                    "percentage": missing_pct,
                }

        # Inflation data coverage
        inflation_coverage = {}
        if "cpi_u" in df.columns:
            inflation_coverage["cpi_u"] = (
                (~df["cpi_u"].isna()).sum() / total_records * 100
            )
        else:
            inflation_coverage["cpi_u"] = 0.0

        if "pce" in df.columns:
            inflation_coverage["pce"] = (~df["pce"].isna()).sum() / total_records * 100
        else:
            inflation_coverage["pce"] = 0.0

        # Report findings
        self.logger.info(f"📈 {stage_prefix}dataset summary:")
        self.logger.info(f"   Total records: {total_records:,}")
        self.logger.info(f"   Unique dates: {unique_dates:,}")
        self.logger.info(f"   Unique people: {unique_people:,}")
        self.logger.info(f"   Date range: {date_range}")
        self.logger.info(f"   Records per date (avg): {total_records/unique_dates:.1f}")

        self.logger.info(f"🎯 {stage_prefix}data quality:")
        for col, stats in missing_analysis.items():
            if stats["count"] > 0:
                self.logger.warning(
                    f"   {col}: {stats['count']:,} missing ({stats['percentage']:.1f}%)"
                )
            else:
                self.logger.info(f"   {col}: Complete ✅")

        self.logger.info(f"🏛️ {stage_prefix}inflation data coverage:")
        self.logger.info(f"   CPI-U: {inflation_coverage['cpi_u']:.1f}%")
        self.logger.info(f"   PCE: {inflation_coverage['pce']:.1f}%")

        return {
            "total_records": total_records,
            "unique_dates": unique_dates,
            "unique_people": unique_people,
            "missing_analysis": missing_analysis,
            "inflation_coverage": inflation_coverage,
        }

    def _report_improvements(
        self, initial_count, final_count, initial_quality, final_quality
    ):
        """Report on data quality improvements."""
        self.logger.info("📊 Data maintenance summary:")

        record_change = final_count - initial_count
        if record_change != 0:
            self.logger.info(
                f"   Records: {initial_count:,} → {final_count:,} ({record_change:+,})"
            )
        else:
            self.logger.info(f"   Records: {final_count:,} (no change)")

        # Inflation coverage improvements
        for metric in ["cpi_u", "pce"]:
            initial_cov = initial_quality["inflation_coverage"][metric]
            final_cov = final_quality["inflation_coverage"][metric]
            improvement = final_cov - initial_cov

            if improvement > 0.1:  # Only report significant improvements
                self.logger.info(
                    f"   {metric.upper()} coverage: {initial_cov:.1f}% → {final_cov:.1f}% (+{improvement:.1f}%)"
                )


def setup_logging(log_file="maintenance.log"):
    """Configure logging for maintenance operations."""
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_path), logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


def main():
    """Main maintenance pipeline execution."""
    parser = argparse.ArgumentParser(
        description="Standalone data maintenance for RedFlagProfits"
    )
    parser.add_argument(
        "--data-file",
        default="data/all_billionaires.parquet",
        help="Path to parquet data file",
    )
    parser.add_argument(
        "--log-file", default="maintenance.log", help="Path to log file"
    )
    parser.add_argument(
        "--skip-duplicates",
        action="store_true",
        help="Skip duplicate detection and merging",
    )
    parser.add_argument(
        "--skip-inflation", action="store_true", help="Skip inflation data updates"
    )
    parser.add_argument(
        "--skip-analysis", action="store_true", help="Skip data quality analysis"
    )
    parser.add_argument(
        "--duplicates-only", action="store_true", help="Run only duplicate handling"
    )
    parser.add_argument(
        "--inflation-only", action="store_true", help="Run only inflation data updates"
    )

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.log_file)
    logger.info("🔧 Starting RedFlagProfits standalone data maintenance")

    # Determine which operations to run
    operations = []

    if args.duplicates_only:
        operations = ["duplicates", "analysis"]
    elif args.inflation_only:
        operations = ["inflation", "analysis"]
    else:
        # Default: run all operations unless specifically skipped
        if not args.skip_duplicates:
            operations.append("duplicates")
        if not args.skip_inflation:
            operations.append("inflation")
        if not args.skip_analysis:
            operations.append("analysis")

    logger.info(f"📋 Operations to run: {', '.join(operations)}")

    try:
        # Check if data file exists
        data_path = Path(args.data_file)
        if not data_path.exists():
            logger.error(f"❌ Dataset not found: {data_path}")
            logger.info("💡 Make sure the data file path is correct")
            return False

        # Initialize maintenance
        maintenance = StandaloneDataMaintenance(logger)

        # Run maintenance operations
        success = maintenance.run_full_maintenance(data_path, operations)

        if success:
            logger.info("🎉 Data maintenance completed successfully!")
            logger.info(f"📁 Updated dataset: {data_path}")
            logger.info(f"📄 Log file: {args.log_file}")
        else:
            logger.error("❌ Data maintenance failed")

        return success

    except Exception as e:
        logger.error(f"❌ Maintenance pipeline failed: {e}")
        logger.exception("Full traceback:")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

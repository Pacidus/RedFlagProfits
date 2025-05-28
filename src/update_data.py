#!/usr/bin/env python3
"""
RedFlagProfits Data Update Pipeline

A clean, focused script that fetches billionaire data from Forbes API
and inflation data from FRED API, then stores it in optimized Parquet format.

Pipeline: Forbes API + FRED API → Processing → Parquet Storage
"""

import os
import json
import logging
import requests
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from dataclasses import dataclass
from io import StringIO
import ast


# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================


@dataclass
class Config:
    """Centralized configuration for the data pipeline."""

    # File paths
    DATA_DIR = "data"
    DICT_DIR = "data/dictionaries"
    PARQUET_FILE = "data/all_billionaires.parquet"
    LOG_FILE = "update.log"

    # API endpoints
    FORBES_API = "https://www.forbes.com/forbesapi/person/rtb/0/-estWorthPrev/true.json"
    FRED_API = "https://api.stlouisfed.org/fred/series/observations"

    # FRED series IDs
    CPI_SERIES = "CPIAUCNS"  # CPI-U Not Seasonally Adjusted
    PCE_SERIES = "PCEPI"  # PCE Price Index

    # Parquet settings
    COMPRESSION_LEVEL = 22
    DATA_PAGE_SIZE = 1048576
    DICT_PAGE_SIZE = 1048576

    # Data processing
    FORBES_COLUMNS = [
        "finalWorth",
        "estWorthPrev",
        "privateAssetsWorth",
        "archivedWorth",
        "personName",
        "gender",
        "birthDate",
        "countryOfCitizenship",
        "state",
        "city",
        "source",
        "industries",
        "financialAssets",
    ]

    GENDER_MAP = {"M": 0, "F": 1}
    INFLATION_BUFFER_DAYS = 90

    # Request headers for Forbes API
    HEADERS = {
        "authority": "www.forbes.com",
        "cache-control": "max-age=0",
        "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
    }


# ============================================================================
# LOGGING SETUP
# ============================================================================


def setup_logging():
    """Configure logging for the pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(Config.LOG_FILE), logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


# ============================================================================
# API DATA FETCHERS
# ============================================================================


class ForbesAPIClient:
    """Handles Forbes API data fetching."""

    def __init__(self, logger):
        self.logger = logger

    def fetch_billionaire_data(self):
        """Fetch billionaire data from Forbes API."""
        self.logger.info("Fetching billionaire data from Forbes API...")

        try:
            response = requests.get(Config.FORBES_API, headers=Config.HEADERS)
            response.raise_for_status()

            # Parse JSON and extract data
            raw_data = pd.read_json(StringIO(response.text))
            data = pd.json_normalize(raw_data["personList"]["personsLists"])

            # Extract and format timestamp
            timestamp = pd.to_datetime(data["timestamp"], unit="ms")
            date_str = timestamp.dt.floor("D").unique()[0].strftime("%Y-%m-%d")

            # Select relevant columns and add crawl date
            clean_data = data[Config.FORBES_COLUMNS].copy()
            clean_data["crawl_date"] = pd.to_datetime(date_str)

            self.logger.info(f"✅ Fetched {len(clean_data)} records for {date_str}")
            return clean_data, date_str

        except Exception as e:
            self.logger.error(f"❌ Forbes API error: {e}")
            return None, None


class FREDAPIClient:
    """Handles FRED API data fetching for inflation data."""

    def __init__(self, logger):
        self.logger = logger
        self.api_key = self._get_api_key()

    def _get_api_key(self):
        """Get FRED API key from environment variable."""
        api_key = os.environ.get("FRED_API_KEY")
        if not api_key:
            self.logger.warning(
                "⚠️  FRED_API_KEY not found - inflation data will be skipped"
            )
            self.logger.info("Set with: export FRED_API_KEY='your_key'")
            return None

        self.logger.info("✅ FRED API key found")
        return api_key

    def fetch_series_data(self, series_id, start_date, end_date):
        """Fetch a single FRED series."""
        if not self.api_key:
            return None

        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": start_date,
            "observation_end": end_date,
        }

        try:
            response = requests.get(Config.FRED_API, params=params, timeout=15)
            response.raise_for_status()

            data = response.json()
            if "error_message" in data:
                self.logger.error(
                    f"FRED API error for {series_id}: {data['error_message']}"
                )
                return None

            # Convert to DataFrame and clean
            df = pd.DataFrame(data["observations"])
            df["date"] = pd.to_datetime(df["date"])
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df[df["value"].notna()]  # Remove missing values

            self.logger.info(f"✅ Fetched {len(df)} {series_id} observations")
            return df[["date", "value"]]

        except Exception as e:
            self.logger.error(f"❌ Error fetching {series_id}: {e}")
            return None

    def get_inflation_data(self, target_date):
        """Get CPI-U and PCE values for a specific date."""
        if not self.api_key:
            return None, None

        # Calculate date range
        start = (target_date - timedelta(days=Config.INFLATION_BUFFER_DAYS)).strftime(
            "%Y-%m-%d"
        )
        end = (target_date + timedelta(days=30)).strftime("%Y-%m-%d")

        # Fetch both series
        cpi_data = self.fetch_series_data(Config.CPI_SERIES, start, end)
        pce_data = self.fetch_series_data(Config.PCE_SERIES, start, end)

        if cpi_data is None or pce_data is None:
            return None, None

        # Match to target month
        target_month = target_date.to_period("M")

        cpi_value = self._match_monthly_value(cpi_data, target_month, "CPI-U")
        pce_value = self._match_monthly_value(pce_data, target_month, "PCE")

        if cpi_value and pce_value:
            self.logger.info(
                f"✅ Inflation data: CPI-U={cpi_value:.1f}, PCE={pce_value:.1f}"
            )

        return cpi_value, pce_value

    def _match_monthly_value(self, data, target_month, series_name):
        """Match monthly inflation data to target month."""
        data_monthly = data.copy()
        data_monthly["year_month"] = data_monthly["date"].dt.to_period("M")

        matches = data_monthly[data_monthly["year_month"] == target_month]
        if len(matches) > 0:
            return float(matches.iloc[0]["value"])

        # Fallback to most recent
        latest_value = float(data.iloc[-1]["value"])
        self.logger.warning(
            f"No {series_name} for {target_month}, using latest: {latest_value}"
        )
        return latest_value


# ============================================================================
# DATA PROCESSING PIPELINE
# ============================================================================


class DataProcessor:
    """Handles data transformation and optimization."""

    def __init__(self, logger):
        self.logger = logger
        self.dictionaries = self._load_dictionaries()

    def _load_dictionaries(self):
        """Load existing mapping dictionaries."""
        dict_names = ["exchanges", "currencies", "industries", "companies"]
        dictionaries = {}

        for name in dict_names:
            dict_path = os.path.join(Config.DICT_DIR, f"{name}.json")
            if os.path.exists(dict_path):
                with open(dict_path, "r") as f:
                    dictionaries[name] = json.load(f)
            else:
                dictionaries[name] = {}

        return dictionaries

    def process_forbes_data(self, df):
        """Transform Forbes data for efficient storage."""
        self.logger.info("🔄 Processing Forbes data...")

        processed_df = df.copy()

        # Process complex fields
        processed_df = self._process_financial_assets(processed_df)
        processed_df = self._process_industries(processed_df)
        processed_df = self._process_simple_fields(processed_df)
        processed_df = self._add_date_components(processed_df)

        self.logger.info("✅ Forbes data processing complete")
        return processed_df

    def _process_financial_assets(self, df):
        """Convert financial assets to columnar format."""
        if "financialAssets" not in df.columns:
            return df

        self.logger.info("  📊 Processing financial assets...")

        # Debug: Check the first few values
        sample_assets = df["financialAssets"].head(3)
        self.logger.info(f"  📋 Sample asset types: {[type(x) for x in sample_assets]}")

        # Debug: Check a specific problematic value
        for i, asset in enumerate(sample_assets):
            self.logger.info(
                f"  📋 Asset {i}: type={type(asset)}, value preview={str(asset)[:100]}"
            )

        # Parse asset strings to Python objects - process one by one to catch errors
        try:
            self.logger.info("  🔄 Parsing financial assets...")

            # Process row by row to catch specific errors
            parsed_assets = []
            for idx, asset_value in df["financialAssets"].items():
                try:
                    parsed_value = self._parse_complex_field(asset_value)
                    parsed_assets.append(parsed_value)
                except Exception as e:
                    self.logger.error(f"  ❌ Error parsing asset at index {idx}: {e}")
                    self.logger.error(f"  📋 Asset type: {type(asset_value)}")
                    self.logger.error(f"  📋 Asset value: {str(asset_value)[:200]}")
                    # Use empty list for problematic entries
                    parsed_assets.append([])

            df["financialAssets"] = parsed_assets
            self.logger.info("  ✅ Financial assets parsed")
        except Exception as e:
            self.logger.error(f"  ❌ Error in parsing loop: {e}")
            raise

        # Convert to columnar format with error handling
        try:
            self.logger.info("  🔄 Extracting asset columns...")
            assets_data = df["financialAssets"].apply(self._extract_asset_columns)
            self.logger.info("  ✅ Asset columns extracted")
        except Exception as e:
            self.logger.error(f"  ❌ Error extracting asset columns: {e}")
            # Create empty columns if processing fails
            empty_columns = {
                "exchanges": [],
                "tickers": [],
                "companies": [],
                "shares": [],
                "prices": [],
                "currencies": [],
                "exchange_rates": [],
            }
            assets_data = pd.Series([empty_columns] * len(df))

        # Add new columns
        try:
            self.logger.info("  🔄 Adding asset columns to dataframe...")
            for column in [
                "exchanges",
                "tickers",
                "companies",
                "shares",
                "prices",
                "currencies",
                "exchange_rates",
            ]:
                df[f"asset_{column}"] = assets_data.apply(lambda x: x.get(column, []))
            self.logger.info("  ✅ Asset columns added to dataframe")
        except Exception as e:
            self.logger.error(f"  ❌ Error adding asset columns: {e}")
            raise

        return df.drop("financialAssets", axis=1)

    def _extract_asset_columns(self, assets_list):
        """Extract asset data into separate column arrays."""
        columns = {
            "exchanges": [],
            "tickers": [],
            "companies": [],
            "shares": [],
            "prices": [],
            "currencies": [],
            "exchange_rates": [],
        }

        # Handle various input types safely
        if assets_list is None:
            return columns
        if not isinstance(assets_list, list):
            return columns
        if len(assets_list) == 0:
            return columns

        for asset in assets_list:
            if not isinstance(asset, dict):
                continue

            # Encode categorical data with safe extraction
            exchanges_val = asset.get("exchange", "")
            companies_val = asset.get("companyName", "")
            currencies_val = asset.get("currencyCode", "")

            columns["exchanges"].append(self._add_to_dict("exchanges", exchanges_val))
            columns["companies"].append(self._add_to_dict("companies", companies_val))
            columns["currencies"].append(
                self._add_to_dict("currencies", currencies_val)
            )

            # Store direct values with safe conversion
            columns["tickers"].append(str(asset.get("ticker", "")))

            try:
                shares_val = float(asset.get("numberOfShares", 0) or 0)
            except (ValueError, TypeError):
                shares_val = 0.0
            columns["shares"].append(shares_val)

            try:
                price_val = float(asset.get("sharePrice", 0) or 0)
            except (ValueError, TypeError):
                price_val = 0.0
            columns["prices"].append(price_val)

            try:
                rate_val = float(asset.get("exchangeRate", 1.0) or 1.0)
            except (ValueError, TypeError):
                rate_val = 1.0
            columns["exchange_rates"].append(rate_val)

        return columns

    def _process_industries(self, df):
        """Convert industries to encoded format."""
        if "industries" not in df.columns:
            return df

        self.logger.info("  🏭 Processing industries...")

        df["industries"] = df["industries"].apply(self._parse_complex_field)
        df["industry_codes"] = df["industries"].apply(
            lambda industries: (
                [self._add_to_dict("industries", ind) for ind in industries]
                if isinstance(industries, list) and industries
                else []
            )
        )

        return df.drop("industries", axis=1)

    def _process_simple_fields(self, df):
        """Process simple field transformations."""
        self.logger.info("  🔤 Processing simple fields...")

        # Convert dates
        if "birthDate" in df.columns:
            df["birthDate"] = pd.to_datetime(
                df["birthDate"], unit="ms", errors="coerce"
            )

        # Encode gender safely
        if "gender" in df.columns:
            df["gender"] = df["gender"].apply(
                lambda x: Config.GENDER_MAP.get(x, -1) if pd.notna(x) else -1
            )

        # Encode country safely
        if "countryOfCitizenship" in df.columns:
            df["country_code"] = df["countryOfCitizenship"].apply(
                lambda x: self._add_to_dict("countries", x) if pd.notna(x) and x else -1
            )
            df = df.drop("countryOfCitizenship", axis=1)

        # Encode source safely
        if "source" in df.columns:
            df["source_code"] = df["source"].apply(
                lambda x: self._add_to_dict("sources", x) if pd.notna(x) and x else -1
            )
            df = df.drop("source", axis=1)

        return df

    def _add_date_components(self, df):
        """Add year/month/day components for efficient filtering."""
        df["year"] = df["crawl_date"].dt.year
        df["month"] = df["crawl_date"].dt.month
        df["day"] = df["crawl_date"].dt.day
        return df

    def add_inflation_data(self, df, cpi_value, pce_value):
        """Add inflation data columns."""
        df["cpi_u"] = cpi_value if cpi_value is not None else np.nan
        df["pce"] = pce_value if pce_value is not None else np.nan

        if cpi_value and pce_value:
            self.logger.info(
                f"✅ Added inflation data: CPI-U={cpi_value:.1f}, PCE={pce_value:.1f}"
            )
        else:
            self.logger.warning("⚠️  Inflation data unavailable - set to NaN")

        return df

    def _add_to_dict(self, dict_name, value):
        """Add value to dictionary and return encoded integer."""
        # Handle pandas/numpy types and empty values explicitly
        if value is None:
            return -1

        # Handle pandas NA values carefully to avoid array boolean ambiguity
        try:
            if pd.isna(value):
                return -1
        except (ValueError, TypeError):
            # pd.isna failed (maybe it's an array), continue with other checks
            pass

        # Convert to string if needed
        if not isinstance(value, str):
            try:
                value = str(value)
            except:
                return -1

        # Skip empty strings after conversion
        if value == "" or value == "nan" or value == "None":
            return -1

        if dict_name not in self.dictionaries:
            self.dictionaries[dict_name] = {}

        if value not in self.dictionaries[dict_name]:
            self.dictionaries[dict_name][value] = len(self.dictionaries[dict_name])

        return self.dictionaries[dict_name][value]

    def _parse_complex_field(self, field_value):
        """Parse complex nested fields from string format."""
        # Handle None first
        if field_value is None:
            return []

        # Handle pandas Series/arrays by converting to regular Python objects
        if hasattr(field_value, "tolist"):
            try:
                converted = field_value.tolist()
                return converted if isinstance(converted, list) else [converted]
            except:
                return []

        # Handle regular pandas NA values (scalar)
        try:
            if pd.isna(field_value):
                return []
        except (ValueError, TypeError):
            # pd.isna failed, continue with other checks
            pass

        # Handle empty strings
        if isinstance(field_value, str) and field_value == "":
            return []

        # If it's already a list, return it
        if isinstance(field_value, list):
            return field_value

        # If it's not a string, try to convert or return as single item
        if not isinstance(field_value, str):
            try:
                return [field_value]
            except:
                return []

        # Parse string representations
        try:
            result = ast.literal_eval(field_value)
            if isinstance(result, list):
                return result
            else:
                return [result] if result is not None else []
        except (ValueError, SyntaxError, TypeError):
            try:
                result = json.loads(field_value)
                if isinstance(result, list):
                    return result
                else:
                    return [result] if result is not None else []
            except (json.JSONDecodeError, TypeError):
                return [field_value] if field_value else []

    def save_dictionaries(self):
        """Save updated mapping dictionaries."""
        self.logger.info("💾 Saving dictionary mappings...")

        os.makedirs(Config.DICT_DIR, exist_ok=True)

        for dict_name, dict_data in self.dictionaries.items():
            dict_path = os.path.join(Config.DICT_DIR, f"{dict_name}.json")
            with open(dict_path, "w") as f:
                json.dump(dict_data, f, indent=2)

        self.logger.info("✅ Dictionary mappings saved")


# ============================================================================
# PARQUET FILE MANAGEMENT
# ============================================================================


class ParquetManager:
    """Handles Parquet file operations."""

    def __init__(self, logger):
        self.logger = logger

    def save_parquet(self, df, filepath):
        """Save DataFrame to Parquet with optimal compression."""
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

    def update_main_dataset(self, new_df, date_str):
        """Update the main parquet dataset with new data."""
        self.logger.info("💾 Updating main parquet dataset...")

        try:
            # Merge with existing data
            if os.path.exists(Config.PARQUET_FILE):
                existing_df = pd.read_parquet(Config.PARQUET_FILE)

                # Remove any existing data for this date
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
                    f"✅ Updated main dataset: {len(combined_df):,} total records"
                )
                self.logger.info(f"📦 File size: {file_size_mb:.2f} MB")

            else:
                # First time - create the main file
                self.save_parquet(new_df, Config.PARQUET_FILE)
                self.logger.info("✅ Created initial parquet dataset")

            return True

        except Exception as e:
            self.logger.error(f"❌ Failed to update parquet dataset: {e}")
            return False


# ============================================================================
# MAIN PIPELINE
# ============================================================================


class DataUpdatePipeline:
    """Main pipeline orchestrator."""

    def __init__(self):
        self.logger = setup_logging()
        self.forbes_client = ForbesAPIClient(self.logger)
        self.fred_client = FREDAPIClient(self.logger)
        self.processor = DataProcessor(self.logger)
        self.parquet_manager = ParquetManager(self.logger)

    def run(self):
        """Execute the complete data update pipeline."""
        self.logger.info("🚀 Starting RedFlagProfits data update pipeline")

        # Ensure directories exist
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        os.makedirs(Config.DICT_DIR, exist_ok=True)

        try:
            # Step 1: Fetch Forbes data
            forbes_data, date_str = self.forbes_client.fetch_billionaire_data()
            if forbes_data is None:
                self.logger.error("❌ Pipeline failed: No Forbes data")
                return False

            # Step 2: Fetch inflation data
            crawl_date = forbes_data["crawl_date"].iloc[0]
            cpi_value, pce_value = self.fred_client.get_inflation_data(crawl_date)

            # Step 3: Process data
            processed_data = self.processor.process_forbes_data(forbes_data)
            processed_data = self.processor.add_inflation_data(
                processed_data, cpi_value, pce_value
            )

            # Step 4: Save to Parquet
            success = self.parquet_manager.update_main_dataset(processed_data, date_str)
            if not success:
                return False

            # Step 5: Save dictionaries
            self.processor.save_dictionaries()

            self.logger.info("🎉 Pipeline completed successfully!")
            return True

        except Exception as e:
            self.logger.error(f"❌ Pipeline failed with error: {e}")
            return False


# ============================================================================
# ENTRY POINT
# ============================================================================


def main():
    """Main entry point."""
    pipeline = DataUpdatePipeline()
    return pipeline.run()


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

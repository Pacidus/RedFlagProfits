"""API clients for Forbes and FRED data."""

import os
import requests
import pandas as pd
from datetime import timedelta
from io import StringIO
import time
from pathlib import Path

from .config import Config


class ForbesClient:
    """Fetches billionaire data from Forbes API."""

    def __init__(self, logger):
        self.logger = logger

    def fetch_data(self):
        """Fetch billionaire data from Forbes API with retry logic."""
        self.logger.info("Fetching billionaire data from Forbes API...")

        for attempt in range(Config.MAX_RETRIES):
            try:
                response = requests.get(
                    Config.FORBES_API,
                    headers=Config.HEADERS,
                    timeout=Config.REQUEST_TIMEOUT,
                )
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

            except (requests.ConnectionError, requests.Timeout) as e:
                self.logger.warning(
                    f"⚠️  Network error (attempt {attempt + 1}/{Config.MAX_RETRIES}): {e}"
                )
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(Config.RETRY_DELAY * (attempt + 1))
                    continue
                self.logger.error("❌ Forbes API network error after all retries")
                return None, None

            except requests.HTTPError as e:
                self.logger.error(f"❌ Forbes API HTTP error: {e}")
                return None, None

            except (KeyError, ValueError) as e:
                self.logger.error(f"❌ Forbes API data parsing error: {e}")
                return None, None

            except Exception as e:
                self.logger.error(f"❌ Unexpected Forbes API error: {e}")
                return None, None

        return None, None


class FredClient:
    """Fetches inflation data from FRED API."""

    def __init__(self, logger):
        self.logger = logger
        self.api_key = self._get_api_key()

    def _get_api_key(self):
        """Get FRED API key from environment."""
        api_key = os.environ.get("FRED_API_KEY")
        if not api_key:
            self.logger.warning(
                "⚠️  FRED_API_KEY not found - inflation data will be skipped"
            )
            return None

        self.logger.info("✅ FRED API key found")
        return api_key

    def get_inflation_data(self, target_date):
        """Get CPI-U and PCE values for target date."""
        if not self.api_key:
            return None, None

        # Ensure target_date is a proper datetime object
        if not isinstance(target_date, pd.Timestamp):
            target_date = pd.to_datetime(target_date)

        # Calculate date range
        start = (target_date - timedelta(days=Config.INFLATION_BUFFER_DAYS)).strftime(
            "%Y-%m-%d"
        )
        end = (target_date + timedelta(days=30)).strftime("%Y-%m-%d")

        # Fetch both series
        cpi_data = self._fetch_series(Config.CPI_SERIES, start, end)
        pce_data = self._fetch_series(Config.PCE_SERIES, start, end)

        if cpi_data is None or pce_data is None:
            return None, None

        # Match to target month - ensure we have a proper datetime before converting to period
        try:
            target_month = target_date.to_period("M")
        except AttributeError:
            # Fallback: convert to timestamp first
            target_date = pd.Timestamp(target_date)
            target_month = target_date.to_period("M")

        cpi_value = self._get_monthly_value(cpi_data, target_month, "CPI-U")
        pce_value = self._get_monthly_value(pce_data, target_month, "PCE")

        if cpi_value and pce_value:
            self.logger.info(
                f"✅ Inflation data: CPI-U={cpi_value:.1f}, PCE={pce_value:.1f}"
            )

        return cpi_value, pce_value

    def _fetch_series(self, series_id, start_date, end_date):
        """Fetch a single FRED series with retry logic."""
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": start_date,
            "observation_end": end_date,
        }

        for attempt in range(Config.MAX_RETRIES):
            try:
                response = requests.get(
                    Config.FRED_API, params=params, timeout=Config.REQUEST_TIMEOUT
                )
                response.raise_for_status()

                data = response.json()
                if "error_message" in data:
                    self.logger.error(
                        f"FRED API error for {series_id}: {data['error_message']}"
                    )
                    return None

                # Convert to DataFrame and clean
                df = pd.DataFrame(data["observations"])
                # Ensure date column is properly converted to datetime
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                df["value"] = pd.to_numeric(df["value"], errors="coerce")
                df = df[df["value"].notna()]

                self.logger.info(f"✅ Fetched {len(df)} {series_id} observations")
                return df[["date", "value"]]

            except (requests.ConnectionError, requests.Timeout) as e:
                self.logger.warning(
                    f"⚠️  FRED network error for {series_id} (attempt {attempt + 1}/{Config.MAX_RETRIES}): {e}"
                )
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(Config.RETRY_DELAY * (attempt + 1))
                    continue
                self.logger.error(
                    f"❌ FRED API network error for {series_id} after all retries"
                )
                return None

            except requests.HTTPError as e:
                self.logger.error(f"❌ FRED API HTTP error for {series_id}: {e}")
                return None

            except (KeyError, ValueError) as e:
                self.logger.error(
                    f"❌ FRED API data parsing error for {series_id}: {e}"
                )
                return None

            except Exception as e:
                self.logger.error(f"❌ Unexpected FRED API error for {series_id}: {e}")
                return None

        return None

    def _get_monthly_value(self, data, target_month, series_name):
        """Get value for target month."""
        data_monthly = data.copy()

        try:
            # Force conversion to datetime and explicitly set the dtype
            if data_monthly["date"].dtype == "object":
                # If it's object dtype, force convert and set proper dtype
                data_monthly["date"] = pd.to_datetime(
                    data_monthly["date"], errors="coerce"
                )
                # Explicitly convert to datetime64[ns] dtype
                data_monthly["date"] = data_monthly["date"].astype("datetime64[ns]")
            elif not pd.api.types.is_datetime64_any_dtype(data_monthly["date"]):
                data_monthly["date"] = pd.to_datetime(
                    data_monthly["date"], errors="coerce"
                )

            # Remove any rows where date conversion failed
            data_monthly = data_monthly.dropna(subset=["date"])

            if len(data_monthly) == 0:
                self.logger.warning(f"No valid dates found for {series_name}")
                return None

            # Now safely use dt accessor
            data_monthly["year_month"] = data_monthly["date"].dt.to_period("M")

            matches = data_monthly[data_monthly["year_month"] == target_month]
            if len(matches) > 0:
                value = float(matches.iloc[0]["value"])
                self.logger.info(
                    f"✅ Found {series_name} value for {target_month}: {value}"
                )
                return value

            # Fallback to most recent
            latest_value = float(data.iloc[-1]["value"])
            self.logger.warning(
                f"No {series_name} for {target_month}, using latest: {latest_value}"
            )
            return latest_value

        except Exception as e:
            self.logger.error(f"Error processing dates for {series_name}: {e}")
            # Emergency fallback - just use the most recent value
            try:
                latest_value = float(data.iloc[-1]["value"])
                self.logger.warning(
                    f"Using emergency fallback for {series_name}: {latest_value}"
                )
                return latest_value
            except Exception as fallback_error:
                self.logger.error(
                    f"Emergency fallback failed for {series_name}: {fallback_error}"
                )
                return None

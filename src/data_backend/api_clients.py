"""Simplified API clients - cleaner without unnecessary complexity."""

import os
import requests
import pandas as pd
from datetime import timedelta
from io import StringIO
import time

from .config import Config


class ForbesClient:
    """Fetches billionaire data from Forbes API."""

    def __init__(self, logger):
        self.logger = logger

    def fetch_data(self):
        """Fetch billionaire data from Forbes API with retry logic."""
        self.logger.info("Fetching billionaire data from Forbes API...")

        # Simple retry loop - clear and direct
        for attempt in range(Config.MAX_RETRIES):
            try:
                response = requests.get(
                    Config.FORBES_API,
                    headers=Config.HEADERS,
                    timeout=Config.REQUEST_TIMEOUT,
                )
                response.raise_for_status()

                # Process successful response
                return self._process_response(response)

            except (requests.ConnectionError, requests.Timeout) as e:
                self.logger.warning(
                    f"⚠️  Network error (attempt {attempt + 1}/{Config.MAX_RETRIES}): {e}"
                )
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(Config.RETRY_DELAY * (attempt + 1))
                    continue
                self.logger.error("❌ Forbes API network error after all retries")

            except requests.HTTPError as e:
                self.logger.error(f"❌ Forbes API HTTP error: {e}")
                break

            except (KeyError, ValueError) as e:
                self.logger.error(f"❌ Forbes API data parsing error: {e}")
                break

        return None, None

    def _process_response(self, response):
        """Process API response into clean DataFrame."""
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

        # Get values for target month
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

        # Simple retry loop - same pattern as Forbes but tailored for FRED
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
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                df["value"] = pd.to_numeric(df["value"], errors="coerce")
                df = df.dropna(subset=["date", "value"])

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

            except requests.HTTPError as e:
                self.logger.error(f"❌ FRED API HTTP error for {series_id}: {e}")
                break

        return None

    def _get_monthly_value(self, data, target_month, series_name):
        """Get value for target month."""
        try:
            # Simple approach - convert dates and match
            data_copy = data.copy()
            data_copy["year_month"] = data_copy["date"].dt.to_period("M")

            # Look for exact match first
            matches = data_copy[data_copy["year_month"] == target_month]
            if len(matches) > 0:
                value = float(matches.iloc[0]["value"])
                self.logger.info(
                    f"✅ Found {series_name} value for {target_month}: {value}"
                )
                return value

            # Fallback to most recent
            if len(data_copy) > 0:
                latest_value = float(data_copy.iloc[-1]["value"])
                self.logger.warning(
                    f"No {series_name} for {target_month}, using latest: {latest_value}"
                )
                return latest_value

        except Exception as e:
            self.logger.error(f"Error processing {series_name} data: {e}")

        return None

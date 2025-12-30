"""Simplified API clients with reduced redundancy."""

import os
import requests
import pandas as pd
from datetime import timedelta
from io import StringIO

from .config import Config
from .utils import retry_on_network_error


class ForbesClient:
    """Fetches billionaire data from Forbes API."""

    def __init__(self, logger):
        self.logger = logger

    def fetch_data(self):
        """Fetch billionaire data from Forbes API with retry logic."""
        self.logger.info("Fetching billionaire data from Forbes API...")
        data = self._fetch_with_retry()
        return self._process_response(data) if data else (None, None)

    @retry_on_network_error(logger=None, operation_name="Forbes API")
    def _fetch_with_retry(self):
        """Fetch data with automatic retry handling."""
        response = requests.get(
            Config.FORBES_API,
            headers=Config.HEADERS,
            timeout=Config.REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response

    def _process_response(self, response):
        """Process API response into clean DataFrame."""
        raw_data = pd.read_json(StringIO(response.text))
        data = pd.json_normalize(raw_data["personList"]["personsLists"])

        timestamp = pd.to_datetime(data["timestamp"], unit="ms")
        date_str = timestamp.dt.floor("D").unique()[0].strftime("%Y-%m-%d")

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
        else:
            self.logger.info("✅ FRED API key found")
        return api_key

    def get_inflation_data(self, target_date):
        """Get CPI-U and PCE values for target date."""
        if not self.api_key:
            return None, None

        target_date = pd.to_datetime(target_date)
        start = target_date - timedelta(days=Config.INFLATION_BUFFER_DAYS)
        end = target_date + timedelta(days=30)
        date_range = (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

        cpi_data = self._fetch_series(Config.CPI_SERIES, *date_range)
        pce_data = self._fetch_series(Config.PCE_SERIES, *date_range)

        if not (cpi_data is not None and pce_data is not None):
            return None, None

        target_month = target_date.to_period("M")
        cpi_value = self._get_monthly_value(cpi_data, target_month, "CPI-U")
        pce_value = self._get_monthly_value(pce_data, target_month, "PCE")

        if cpi_value and pce_value:
            self.logger.info(
                f"✅ Inflation data: CPI-U={cpi_value:.1f}, PCE={pce_value:.1f}"
            )

        return cpi_value, pce_value

    @retry_on_network_error(logger=None, operation_name="FRED API")
    def _fetch_series(self, series_id, start_date, end_date):
        """Fetch a single FRED series with retry logic."""
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": start_date,
            "observation_end": end_date,
        }

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

        # Defensive check: ensure observations exist and have data
        observations = data.get("observations", [])
        if not observations:
            self.logger.warning(
                f"⚠️  No observations returned for {series_id} between {start_date} and {end_date}"
            )
            return None

        df = pd.DataFrame(observations)

        # Defensive check: ensure required columns exist
        if "date" not in df.columns or "value" not in df.columns:
            self.logger.warning(
                f"⚠️  Missing required columns in {series_id} response. "
                f"Available columns: {list(df.columns)}"
            )
            return None

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["date", "value"])

        if df.empty:
            self.logger.warning(f"⚠️  No valid data after parsing for {series_id}")
            return None

        self.logger.info(f"✅ Fetched {len(df)} {series_id} observations")
        return df[["date", "value"]]

    def _get_monthly_value(self, data, target_month, series_name):
        """Get value for target month."""
        try:
            data_copy = data.copy()
            data_copy["year_month"] = data_copy["date"].dt.to_period("M")

            matches = data_copy[data_copy["year_month"] == target_month]
            if len(matches) > 0:
                value = float(matches.iloc[0]["value"])
                self.logger.info(
                    f"✅ Found {series_name} value for {target_month}: {value}"
                )
                return value

            if len(data_copy) > 0:
                latest_value = float(data_copy.iloc[-1]["value"])
                self.logger.warning(
                    f"No {series_name} for {target_month}, using latest: {latest_value}"
                )
                return latest_value

        except Exception as e:
            self.logger.error(f"Error processing {series_name} data: {e}")

        return None

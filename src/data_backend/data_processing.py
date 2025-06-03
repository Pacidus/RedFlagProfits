"""Simplified data transformation and processing utilities."""

import json
import pandas as pd
import numpy as np
import ast

from .config import Config
from .utils import is_invalid_value, safe_numeric_conversion


class DataProcessor:
    """Handles data transformation and encoding with simplified patterns."""

    def __init__(self, logger):
        self.logger = logger
        self.dictionaries = self._load_dictionaries()

    def _load_dictionaries(self):
        """Load existing mapping dictionaries."""
        dictionaries = {}
        for name in Config.DICTIONARY_NAMES:
            dict_path = Config.DICT_DIR / f"{name}.json"
            dictionaries[name] = {}
            if dict_path.exists():
                try:
                    with dict_path.open("r") as f:
                        dictionaries[name] = json.load(f)
                except (json.JSONDecodeError, IOError) as e:
                    self.logger.warning(f"⚠️  Could not load {name} dictionary: {e}")
        return dictionaries

    def process_data(self, df):
        """Main processing pipeline with consolidated operations."""
        self.logger.info("🔄 Processing Forbes data...")
        return (
            df.pipe(self._process_financial_assets)
            .pipe(self._process_industries_and_fields)
            .pipe(self._add_date_components)
        )

    def add_inflation_data(self, df, cpi_value, pce_value):
        """Add inflation columns with status logging."""
        df["cpi_u"] = cpi_value or np.nan
        df["pce"] = pce_value or np.nan

        status = "✅" if cpi_value and pce_value else "⚠️"
        self.logger.info(
            f"{status} Inflation data: "
            f"CPI-U={cpi_value or 'Unavailable'}, PCE={pce_value or 'Unavailable'}"
        )
        return df

    def save_dictionaries(self):
        """Save updated dictionaries."""
        self.logger.info("💾 Saving dictionary mappings...")
        try:
            Config.DICT_DIR.mkdir(parents=True, exist_ok=True)
            for dict_name, dict_data in self.dictionaries.items():
                with (Config.DICT_DIR / f"{dict_name}.json").open("w") as f:
                    json.dump(dict_data, f, indent=2)
            self.logger.info("✅ Dictionary mappings saved")
        except IOError as e:
            self.logger.error(f"❌ Failed to save dictionaries: {e}")

    def _process_financial_assets(self, df):
        """Process financial assets with consolidated extraction."""
        if "financialAssets" not in df.columns:
            return df

        self.logger.info("  📊 Processing financial assets...")
        assets_series = df["financialAssets"].apply(self._parse_complex_field)

        for col in Config.ASSET_COLUMNS:
            df[f"asset_{col}"] = assets_series.apply(
                lambda assets: self._extract_assets(assets, col)
            )
        return df.drop("financialAssets", axis=1)

    def _process_industries_and_fields(self, df):
        """Process industries and simple fields in one pass."""
        self.logger.info("  🏭 Processing industries and fields...")

        # Process industries
        if "industries" in df.columns:
            df["industry_codes"] = (
                df["industries"]
                .apply(self._parse_complex_field)
                .apply(
                    lambda x: (
                        [self._encode_value("industries", i) for i in x]
                        if isinstance(x, list) and x
                        else []
                    )
                )
            )
            df = df.drop("industries", axis=1)

        # Process birthDate with safe conversion
        if "birthDate" in df.columns:
            df["birthDate"] = self._safe_datetime_conversion(df["birthDate"])

        if "gender" in df.columns:
            df["gender"] = df["gender"].apply(
                lambda x: (
                    Config.GENDER_MAP.get(str(x).upper(), Config.INVALID_CODE)
                    if not pd.isna(x)
                    else Config.INVALID_CODE
                )
            )

        # Process column mappings
        for old_col, dict_name, new_col in Config.COLUMN_MAPPINGS:
            if old_col in df.columns:
                df[new_col] = df[old_col].apply(
                    lambda x: self._encode_value(dict_name, x)
                )
                df = df.drop(old_col, axis=1)

        return df

    def _safe_datetime_conversion(self, series):
        """Safely convert birthDate values to datetime, handling overflow."""
        # Convert to numeric first, coercing errors to NaN
        numeric_series = pd.to_numeric(series, errors="coerce")

        # Define reasonable bounds for birth dates (in milliseconds since epoch)
        # 1900-01-01: -2208988800000 ms
        # Today: calculated dynamically
        min_timestamp_ms = -2209032000  # ~1900
        max_timestamp_ms = int(pd.Timestamp.now().timestamp() * 1000)  # Today

        # Filter out values outside reasonable range
        valid_mask = (
            numeric_series.notna()
            & (numeric_series >= min_timestamp_ms)
            & (numeric_series <= max_timestamp_ms)
        )

        # Create result series filled with NaT
        result = pd.Series(pd.NaT, index=series.index)

        # Convert only valid values
        if valid_mask.any():
            valid_values = numeric_series[valid_mask]
            result[valid_mask] = pd.to_datetime(
                valid_values, unit="ms", errors="coerce"
            )

        return result

    def _extract_assets(self, assets_list, col):
        """Extract specific asset column data with simplified logic."""
        if not isinstance(assets_list, list):
            return []

        # Define extraction mappings
        extraction_map = {
            "exchanges": ("exchange", True),
            "companies": ("companyName", True),
            "currencies": ("currencyCode", True),
            "tickers": ("ticker", False),
        }

        if col in extraction_map:
            key, encode = extraction_map[col]
            return [
                (
                    self._encode_value(col, asset.get(key, ""))
                    if encode
                    else str(asset.get(key, ""))
                )
                for asset in assets_list
                if isinstance(asset, dict)
            ]

        # Handle numeric fields
        for field, key, default in Config.ASSET_FIELD_MAPPINGS:
            if field == col:
                return [
                    safe_numeric_conversion(asset.get(key, default), default)
                    for asset in assets_list
                    if isinstance(asset, dict)
                ]

        return []

    def _add_date_components(self, df):
        """Add date components for efficient filtering."""
        if "crawl_date" in df.columns:
            df["crawl_date"] = pd.to_datetime(df["crawl_date"], errors="coerce")
            if not df["crawl_date"].isna().all():
                df["year"] = df["crawl_date"].dt.year
                df["month"] = df["crawl_date"].dt.month
                df["day"] = df["crawl_date"].dt.day
        return df

    def _encode_value(self, dict_name, value):
        """Encode value in dictionary with simplified logic."""
        if is_invalid_value(value):
            return Config.INVALID_CODE

        value_str = str(value)
        dictionary = self.dictionaries[dict_name]
        if value_str not in dictionary:
            dictionary[value_str] = len(dictionary)
        return dictionary[value_str]

    def _parse_complex_field(self, field_value):
        """Parse complex fields with consolidated parsing logic."""
        if is_invalid_value(field_value):
            return []

        if isinstance(field_value, list):
            return field_value

        if isinstance(field_value, str):
            for parser in (ast.literal_eval, json.loads):
                try:
                    result = parser(field_value)
                    return result if isinstance(result, list) else [result]
                except (ValueError, SyntaxError):
                    continue

        return [field_value] if field_value else []

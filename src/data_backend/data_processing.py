"""Data transformation and processing utilities."""

import json
import pandas as pd
import numpy as np
import ast

from .config import Config


class DataProcessor:
    """Handles data transformation and encoding."""

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
        """Main processing pipeline."""
        self.logger.info("🔄 Processing Forbes data...")
        return (
            df.pipe(self._process_financial_assets)
            .pipe(self._process_industries)
            .pipe(self._process_simple_fields)
            .pipe(self._add_date_components)
            .assign(**{"year": 2025, "month": 1, "day": 1})  # Default values
        )

    def add_inflation_data(self, df, cpi_value, pce_value):
        """Add inflation columns."""
        df["cpi_u"] = cpi_value or np.nan
        df["pce"] = pce_value or np.nan

        status = ("✅" if cpi_value and pce_value else "⚠️") + " Inflation data"
        self.logger.info(
            f"{status}: CPI-U={cpi_value or 'Unavailable'}, PCE={pce_value or 'Unavailable'}"
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
        """Process financial assets."""
        if "financialAssets" not in df.columns:
            return df

        self.logger.info("  📊 Processing financial assets...")
        assets_series = df["financialAssets"].apply(self._parse_complex_field)

        for col in Config.ASSET_COLUMNS:
            df[f"asset_{col}"] = assets_series.apply(
                lambda assets: self._extract_assets(assets, col)
            )
        return df.drop("financialAssets", axis=1)

    def _extract_assets(self, assets_list, col):
        """Extract specific asset column data."""
        if not isinstance(assets_list, list):
            return []

        if col in ["exchanges", "companies", "currencies"]:
            key = {
                "exchanges": "exchange",
                "companies": "companyName",
                "currencies": "currencyCode",
            }[col]
            return [
                self._encode_value(col, asset.get(key, ""))
                for asset in assets_list
                if isinstance(asset, dict)
            ]

        if col == "tickers":
            return [
                str(asset.get("ticker", ""))
                for asset in assets_list
                if isinstance(asset, dict)
            ]

        # Process numeric fields
        return [
            self._get_numeric_value(asset, *field_map)
            for asset in assets_list
            if isinstance(asset, dict)
            for field_map in Config.ASSET_FIELD_MAPPINGS
            if field_map[0] == col
        ]

    def _get_numeric_value(self, asset, field, key, default):
        """Safe numeric value extraction."""
        try:
            return float(asset.get(key, default)) or default
        except (ValueError, TypeError):
            return default

    def _process_industries(self, df):
        """Process industries field."""
        if "industries" in df.columns:
            self.logger.info("  🏭 Processing industries...")
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
            return df.drop("industries", axis=1)
        return df

    def _process_simple_fields(self, df):
        """Process simple field transformations."""
        self.logger.info("  🔤 Processing simple fields...")

        # Date conversion
        if "birthDate" in df.columns:
            df["birthDate"] = pd.to_datetime(
                df["birthDate"], unit="ms", errors="coerce"
            )

        # Gender mapping
        if "gender" in df.columns:
            gender_map = {"M": 0, "F": 1}
            df["gender"] = df["gender"].apply(
                lambda x: (
                    gender_map.get(str(x).upper(), Config.INVALID_CODE)
                    if not pd.isna(x)
                    else Config.INVALID_CODE
                )
            )

        # Column encoding and dropping
        column_mappings = [
            ("countryOfCitizenship", "countries", "country_code"),
            ("source", "sources", "source_code"),
        ]
        for old_col, dict_name, new_col in column_mappings:
            if old_col in df.columns:
                df[new_col] = df[old_col].apply(
                    lambda x: self._encode_value(dict_name, x)
                )
                df = df.drop(old_col, axis=1)
        return df

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
        """Encode value in dictionary."""
        if self._is_invalid_value(value):
            return Config.INVALID_CODE

        value_str = str(value)
        dictionary = self.dictionaries[dict_name]
        if value_str not in dictionary:
            dictionary[value_str] = len(dictionary)
        return dictionary[value_str]

    def _is_invalid_value(self, value):
        """Check if value is invalid/empty."""
        return (
            value is None
            or value == ""
            or (hasattr(value, "__len__") and len(str(value)) == 0)
            or (pd.isna(value) if isinstance(value, (int, float)) else False)
        )

    def _parse_complex_field(self, field_value):
        """Parse complex fields from string format."""
        if self._is_invalid_value(field_value):
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

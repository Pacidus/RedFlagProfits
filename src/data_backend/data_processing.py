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
            if dict_path.exists():
                try:
                    with dict_path.open("r") as f:
                        dictionaries[name] = json.load(f)
                except (json.JSONDecodeError, IOError) as e:
                    self.logger.warning(f"⚠️  Could not load {name} dictionary: {e}")
                    dictionaries[name] = {}
            else:
                dictionaries[name] = {}

        return dictionaries

    def process_data(self, df):
        """Main processing pipeline."""
        self.logger.info("🔄 Processing Forbes data...")

        processed_df = df
        processed_df = self._process_financial_assets(processed_df)
        processed_df = self._process_industries(processed_df)
        processed_df = self._process_simple_fields(processed_df)
        processed_df = self._add_date_components(processed_df)

        self.logger.info("✅ Forbes data processing complete")
        return processed_df

    def add_inflation_data(self, df, cpi_value, pce_value):
        """Add inflation columns."""
        df["cpi_u"] = cpi_value if cpi_value is not None else np.nan
        df["pce"] = pce_value if pce_value is not None else np.nan

        if cpi_value and pce_value:
            self.logger.info(
                f"✅ Added inflation data: CPI-U={cpi_value:.1f}, PCE={pce_value:.1f}"
            )
        else:
            self.logger.warning("⚠️  Inflation data unavailable")

        return df

    def save_dictionaries(self):
        """Save updated dictionaries."""
        self.logger.info("💾 Saving dictionary mappings...")

        try:
            Config.DICT_DIR.mkdir(parents=True, exist_ok=True)
            for dict_name, dict_data in self.dictionaries.items():
                dict_path = Config.DICT_DIR / f"{dict_name}.json"
                with dict_path.open("w") as f:
                    json.dump(dict_data, f, indent=2)

            self.logger.info("✅ Dictionary mappings saved")
        except IOError as e:
            self.logger.error(f"❌ Failed to save dictionaries: {e}")

    def _process_financial_assets(self, df):
        """Process financial assets - simplified version."""
        if "financialAssets" not in df.columns:
            return df

        self.logger.info("  📊 Processing financial assets...")

        # Parse assets safely
        df["financialAssets"] = df["financialAssets"].apply(self._safe_parse_assets)

        # Extract asset columns
        assets_data = df["financialAssets"].apply(self._extract_asset_columns)

        # Add columns to dataframe
        for col in Config.ASSET_COLUMNS:
            df[f"asset_{col}"] = assets_data.apply(lambda x: x.get(col, []))

        return df.drop("financialAssets", axis=1)

    def _safe_parse_assets(self, asset_value):
        """Safely parse asset field."""
        try:
            return self._parse_complex_field(asset_value)
        except Exception as e:
            self.logger.debug(f"Asset parsing failed: {e}")
            return []

    def _extract_asset_columns(self, assets_list):
        """Extract asset data into columns."""
        if not isinstance(assets_list, list):
            return {col: [] for col in Config.ASSET_COLUMNS}

        result = {col: [] for col in Config.ASSET_COLUMNS}

        for asset in assets_list:
            if not isinstance(asset, dict):
                continue

            # Encode categorical fields
            result["exchanges"].append(
                self._encode_value("exchanges", asset.get("exchange", ""))
            )
            result["companies"].append(
                self._encode_value("companies", asset.get("companyName", ""))
            )
            result["currencies"].append(
                self._encode_value("currencies", asset.get("currencyCode", ""))
            )
            result["tickers"].append(str(asset.get("ticker", "")))

            # Numeric fields with safe conversion using config
            for field, key, default in Config.ASSET_FIELD_MAPPINGS:
                try:
                    value = float(asset.get(key, default) or default)
                except (ValueError, TypeError):
                    value = default
                result[field].append(value)

        return result

    def _process_industries(self, df):
        """Process industries field."""
        if "industries" not in df.columns:
            return df

        self.logger.info("  🏭 Processing industries...")
        df["industries"] = df["industries"].apply(self._parse_complex_field)
        df["industry_codes"] = df["industries"].apply(
            lambda industries: (
                [self._encode_value("industries", ind) for ind in industries]
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
            # Convert to datetime and explicitly handle the dtype
            df["birthDate"] = pd.to_datetime(
                df["birthDate"], unit="ms", errors="coerce"
            )

        # Encode categorical fields using pattern matching
        if "gender" in df.columns:
            df["gender"] = df["gender"].apply(self._map_gender)

        if "countryOfCitizenship" in df.columns:
            df["country_code"] = df["countryOfCitizenship"].apply(
                lambda x: self._encode_value("countries", x)
            )
            df = df.drop("countryOfCitizenship", axis=1)

        if "source" in df.columns:
            df["source_code"] = df["source"].apply(
                lambda x: self._encode_value("sources", x)
            )
            df = df.drop("source", axis=1)

        return df

    def _map_gender(self, gender_value):
        """Map gender using pattern matching."""
        if pd.isna(gender_value):
            return Config.INVALID_CODE

        match str(gender_value).upper():
            case "M":
                return 0
            case "F":
                return 1
            case _:
                return Config.INVALID_CODE

    def _add_date_components(self, df):
        """Add date components for efficient filtering."""
        # Ensure crawl_date is properly converted to datetime
        if "crawl_date" in df.columns:
            # Check if it's already datetime
            if not pd.api.types.is_datetime64_any_dtype(df["crawl_date"]):
                df["crawl_date"] = pd.to_datetime(df["crawl_date"], errors="coerce")

            # Only proceed if we have valid datetime data
            if pd.api.types.is_datetime64_any_dtype(df["crawl_date"]):
                df["year"] = df["crawl_date"].dt.year
                df["month"] = df["crawl_date"].dt.month
                df["day"] = df["crawl_date"].dt.day
            else:
                self.logger.warning(
                    "⚠️  Could not convert crawl_date to datetime, skipping date components"
                )
                # Add default values
                df["year"] = 2025
                df["month"] = 1
                df["day"] = 1

        return df

    def _encode_value(self, dict_name, value):
        """Encode value in dictionary."""
        if self._is_invalid_value(value):
            return Config.INVALID_CODE

        value_str = str(value)
        if value_str in {"", "nan", "None"}:
            return Config.INVALID_CODE

        if value_str not in self.dictionaries[dict_name]:
            self.dictionaries[dict_name][value_str] = len(self.dictionaries[dict_name])

        return self.dictionaries[dict_name][value_str]

    def _is_invalid_value(self, value):
        """Check if value is invalid/empty."""
        if value is None or value == "":
            return True

        if hasattr(value, "__len__") and len(str(value)) == 0:
            return True

        try:
            if pd.isna(value):
                return True
        except (ValueError, TypeError):
            pass

        return False

    def _parse_complex_field(self, field_value):
        """Parse complex fields from string format."""
        if self._is_invalid_value(field_value):
            return []

        if isinstance(field_value, list):
            return field_value

        if isinstance(field_value, str):
            for parser in [ast.literal_eval, json.loads]:
                try:
                    result = parser(field_value)
                    return result if isinstance(result, list) else [result]
                except (ValueError, SyntaxError):
                    continue
            return [field_value]

        return [field_value] if field_value else []

"""Data transformation and processing utilities."""

import os
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
        dict_names = [
            "exchanges",
            "currencies",
            "industries",
            "companies",
            "countries",
            "sources",
        ]
        dictionaries = {}

        for name in dict_names:
            dict_path = os.path.join(Config.DICT_DIR, f"{name}.json")
            if os.path.exists(dict_path):
                with open(dict_path, "r") as f:
                    dictionaries[name] = json.load(f)
            else:
                dictionaries[name] = {}

        return dictionaries

    def process_data(self, df):
        """Main processing pipeline."""
        self.logger.info("🔄 Processing Forbes data...")

        processed_df = df.copy()
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

        os.makedirs(Config.DICT_DIR, exist_ok=True)
        for dict_name, dict_data in self.dictionaries.items():
            dict_path = os.path.join(Config.DICT_DIR, f"{dict_name}.json")
            with open(dict_path, "w") as f:
                json.dump(dict_data, f, indent=2)

        self.logger.info("✅ Dictionary mappings saved")

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
        for col in [
            "exchanges",
            "tickers",
            "companies",
            "shares",
            "prices",
            "currencies",
            "exchange_rates",
        ]:
            df[f"asset_{col}"] = assets_data.apply(lambda x: x.get(col, []))

        return df.drop("financialAssets", axis=1)

    def _safe_parse_assets(self, asset_value):
        """Safely parse asset field."""
        try:
            return self._parse_complex_field(asset_value)
        except Exception:
            return []

    def _extract_asset_columns(self, assets_list):
        """Extract asset data into columns."""
        if not isinstance(assets_list, list):
            return {
                col: []
                for col in [
                    "exchanges",
                    "tickers",
                    "companies",
                    "shares",
                    "prices",
                    "currencies",
                    "exchange_rates",
                ]
            }

        result = {
            col: []
            for col in [
                "exchanges",
                "tickers",
                "companies",
                "shares",
                "prices",
                "currencies",
                "exchange_rates",
            ]
        }

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

            # Numeric fields with safe conversion
            for field, key, default in [
                ("shares", "numberOfShares", 0.0),
                ("prices", "sharePrice", 0.0),
                ("exchange_rates", "exchangeRate", 1.0),
            ]:
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
            df["birthDate"] = pd.to_datetime(
                df["birthDate"], unit="ms", errors="coerce"
            )

        # Encode categorical fields
        if "gender" in df.columns:
            df["gender"] = df["gender"].apply(
                lambda x: Config.GENDER_MAP.get(x, -1) if pd.notna(x) else -1
            )

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

    def _add_date_components(self, df):
        """Add date components for efficient filtering."""
        df["year"] = df["crawl_date"].dt.year
        df["month"] = df["crawl_date"].dt.month
        df["day"] = df["crawl_date"].dt.day
        return df

    def _encode_value(self, dict_name, value):
        """Encode value in dictionary."""
        if (
            value is None
            or value == ""
            or (hasattr(value, "__len__") and len(str(value)) == 0)
        ):
            return -1

        try:
            if pd.isna(value):
                return -1
        except (ValueError, TypeError):
            pass

        value_str = str(value)
        if value_str in ["", "nan", "None"]:
            return -1

        if value_str not in self.dictionaries[dict_name]:
            self.dictionaries[dict_name][value_str] = len(self.dictionaries[dict_name])

        return self.dictionaries[dict_name][value_str]

    def _parse_complex_field(self, field_value):
        """Parse complex fields from string format."""
        if field_value is None or field_value == "":
            return []

        try:
            if pd.isna(field_value):
                return []
        except (ValueError, TypeError):
            pass

        if isinstance(field_value, list):
            return field_value

        if isinstance(field_value, str):
            for parser in [ast.literal_eval, json.loads]:
                try:
                    result = parser(field_value)
                    return result if isinstance(result, list) else [result]
                except:
                    continue
            return [field_value]

        return [field_value] if field_value else []

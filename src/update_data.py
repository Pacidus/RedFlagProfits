#!/usr/bin/env python3
"""
Parquet Data Update Script for RedFlagProfits

This script fetches new billionaire data and adds it to the parquet dataset.
"""

import requests
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
import json
import logging
from io import StringIO
import ast
import gc

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("update.log"), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


# Load existing dictionaries
def load_dictionaries():
    """Load existing mapping dictionaries."""
    dicts = {}
    dict_path = "data/dictionaries"
    for dict_name in ["exchanges", "currencies", "industries", "companies"]:
        dict_file = os.path.join(dict_path, f"{dict_name}.json")
        if os.path.exists(dict_file):
            with open(dict_file, "r") as f:
                dicts[dict_name] = json.load(f)
    return dicts


# Reused functions from convert_parquet.py
def parse_complex_field(field_value):
    """Parse complex nested fields from string to proper Python structures."""
    if pd.isna(field_value) or field_value == "":
        return []

    try:
        # Try ast.literal_eval for safer parsing
        return ast.literal_eval(field_value)
    except (ValueError, SyntaxError, TypeError):
        try:
            # Fallback to json.loads
            return json.loads(field_value)
        except (json.JSONDecodeError, TypeError):
            # If all else fails, return as is
            return field_value


def process_financial_assets(
    assets_list, COMMON_EXCHANGES, COMMON_COMPANIES, COMMON_CURRENCIES
):
    """Process financial assets to optimize storage in parquet."""
    if not assets_list or not isinstance(assets_list, list):
        return {
            "exchanges": [],
            "tickers": [],
            "company_names": [],
            "shares": [],
            "prices": [],
            "currencies": [],
            "exchange_rates": [],
        }

    # Extract data into separate lists for columnar efficiency
    exchanges = []
    tickers = []
    company_names = []
    shares = []
    prices = []
    currencies = []
    exchange_rates = []

    for asset in assets_list:
        # Normalize exchange
        exchange = asset.get("exchange", "")
        if exchange not in COMMON_EXCHANGES and exchange:
            COMMON_EXCHANGES[exchange] = len(COMMON_EXCHANGES)
        exchanges.append(COMMON_EXCHANGES.get(exchange, -1))

        # Store ticker
        tickers.append(asset.get("ticker", ""))

        # Normalize company name
        company = asset.get("companyName", "")
        if company not in COMMON_COMPANIES and company:
            COMMON_COMPANIES[company] = len(COMMON_COMPANIES)
        company_names.append(COMMON_COMPANIES.get(company, -1))

        # Extract numerical data
        shares.append(float(asset.get("numberOfShares", 0) or 0))
        prices.append(float(asset.get("sharePrice", 0) or 0))

        # Normalize currency
        currency = asset.get("currencyCode", "")
        if currency not in COMMON_CURRENCIES and currency:
            COMMON_CURRENCIES[currency] = len(COMMON_CURRENCIES)
        currencies.append(COMMON_CURRENCIES.get(currency, -1))

        # Store exchange rate
        exchange_rates.append(float(asset.get("exchangeRate", 1.0) or 1.0))

    return {
        "exchanges": exchanges,
        "tickers": tickers,
        "company_names": company_names,
        "shares": shares,
        "prices": prices,
        "currencies": currencies,
        "exchange_rates": exchange_rates,
    }


def process_industries(industries_list, COMMON_INDUSTRIES):
    """Process industries list to optimize storage in parquet."""
    if not industries_list or not isinstance(industries_list, list):
        return []

    result = []
    for industry in industries_list:
        if industry not in COMMON_INDUSTRIES and industry:
            COMMON_INDUSTRIES[industry] = len(COMMON_INDUSTRIES)
        result.append(COMMON_INDUSTRIES.get(industry, -1))

    return result


def process_dataframe(df, dictionaries):
    """Process a DataFrame to prepare it for optimized Parquet storage."""
    COMMON_EXCHANGES = dictionaries.get("exchanges", {})
    COMMON_CURRENCIES = dictionaries.get("currencies", {})
    COMMON_INDUSTRIES = dictionaries.get("industries", {})
    COMMON_COMPANIES = dictionaries.get("companies", {})

    # Process complex columns
    if "financialAssets" in df.columns:
        # Parse string representations to Python objects
        logger.info("Processing financial assets column...")
        df["financialAssets"] = df["financialAssets"].apply(parse_complex_field)

        # Create optimized structure
        assets_data = df["financialAssets"].apply(
            lambda x: process_financial_assets(
                x, COMMON_EXCHANGES, COMMON_COMPANIES, COMMON_CURRENCIES
            )
        )

        # Extract columns for columnar storage
        df["asset_exchanges"] = assets_data.apply(lambda x: x["exchanges"])
        df["asset_tickers"] = assets_data.apply(lambda x: x["tickers"])
        df["asset_companies"] = assets_data.apply(lambda x: x["company_names"])
        df["asset_shares"] = assets_data.apply(lambda x: x["shares"])
        df["asset_prices"] = assets_data.apply(lambda x: x["prices"])
        df["asset_currencies"] = assets_data.apply(lambda x: x["currencies"])
        df["asset_exchange_rates"] = assets_data.apply(lambda x: x["exchange_rates"])

        # Remove original column to save space
        df = df.drop("financialAssets", axis=1)

    # Process industries column
    if "industries" in df.columns:
        logger.info("Processing industries column...")
        df["industries"] = df["industries"].apply(parse_complex_field)
        df["industry_codes"] = df["industries"].apply(
            lambda x: process_industries(x, COMMON_INDUSTRIES)
        )
        df = df.drop("industries", axis=1)

    # Convert birthDate to proper datetime
    if "birthDate" in df.columns:
        logger.info("Converting birthDate to datetime...")
        df["birthDate"] = pd.to_datetime(df["birthDate"], unit="ms", errors="coerce")

    # Normalize gender to integer
    if "gender" in df.columns:
        logger.info("Normalizing gender...")
        gender_map = {"M": 0, "F": 1}
        df["gender"] = df["gender"].map(lambda x: gender_map.get(x, -1))

    # Normalize country and source
    if "countryOfCitizenship" in df.columns:
        countries = {}
        df["country_code"] = df["countryOfCitizenship"].apply(
            lambda x: countries.setdefault(x, len(countries)) if x else -1
        )
        df = df.drop("countryOfCitizenship", axis=1)

    if "source" in df.columns:
        sources = {}
        df["source_code"] = df["source"].apply(
            lambda x: sources.setdefault(x, len(sources)) if x else -1
        )
        df = df.drop("source", axis=1)

    # Return updated dictionaries along with the processed dataframe
    updated_dicts = {
        "exchanges": COMMON_EXCHANGES,
        "currencies": COMMON_CURRENCIES,
        "industries": COMMON_INDUSTRIES,
        "companies": COMMON_COMPANIES,
    }

    return df, updated_dicts


def fetch_new_data():
    """Fetch new data from Forbes API."""
    logger.info("Fetching new data from Forbes API...")

    headers = {
        "authority": "www.forbes.com",
        "cache-control": "max-age=0",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Mobile Safari/537.36",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "sec-fetch-site": "none",
        "sec-fetch-mode": "navigate",
        "sec-fetch-user": "?1",
        "sec-fetch-dest": "document",
        "accept-language": "en-US,en;q=0.9",
    }

    try:
        rqst = requests.get(
            "https://www.forbes.com/forbesapi/person/rtb/0/-estWorthPrev/true.json",
            headers=headers,
        )
        rqst.raise_for_status()

        data = pd.json_normalize(
            pd.read_json(StringIO(rqst.text))["personList"]["personsLists"]
        )

        # Extract timestamp
        time = pd.to_datetime(data["timestamp"], unit="ms")
        time_str = np.datetime_as_string(time.dt.floor("D").unique()[:1], unit="D")[0]

        logger.info(f"Successfully fetched data with timestamp: {time_str}")

        # Select relevant columns
        keys = [
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
        data = data[keys]

        # Add crawl date
        data["crawl_date"] = pd.to_datetime(time_str)

        return data, time_str

    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return None, None


def update_parquet_file(new_df, time_str):
    """Update the parquet file with new data."""
    logger.info("Updating parquet file...")

    # Load existing dictionaries
    dictionaries = load_dictionaries()

    # Process the dataframe
    processed_df, updated_dicts = process_dataframe(new_df, dictionaries)

    # Add date components for potential filtering
    processed_df["year"] = processed_df["crawl_date"].dt.year
    processed_df["month"] = processed_df["crawl_date"].dt.month
    processed_df["day"] = processed_df["crawl_date"].dt.day

    # Path to main parquet file
    parquet_file = "data/all_billionaires.parquet"

    # Save new data as a separate parquet file first
    new_file = f"data/{time_str}.parquet"

    # Convert to PyArrow Table
    table = pa.Table.from_pandas(processed_df)

    # Write to parquet with maximum compression
    pq.write_table(
        table,
        new_file,
        compression="zstd",
        compression_level=22,
        use_dictionary=True,
        write_statistics=True,
        version="2.6",
        data_page_size=1048576,
        dictionary_pagesize_limit=1048576,
    )

    logger.info(f"Saved new data to {new_file}")

    # Now update the main parquet file
    try:
        # Read existing data
        existing_df = pd.read_parquet(parquet_file)

        # Check if we already have data for this date
        if time_str in existing_df["crawl_date"].dt.strftime("%Y-%m-%d").values:
            logger.warning(f"Data for {time_str} already exists in the parquet file")
            # We could either skip or replace - for now, let's replace
            existing_df = existing_df[
                existing_df["crawl_date"].dt.strftime("%Y-%m-%d") != time_str
            ]

        # Combine the dataframes
        combined_df = pd.concat([existing_df, processed_df], ignore_index=True)

        # Sort by date
        combined_df = combined_df.sort_values(by=["year", "month", "day", "personName"])

        # Convert to Arrow table
        combined_table = pa.Table.from_pandas(combined_df)

        # Write updated file
        pq.write_table(
            combined_table,
            parquet_file,
            compression="zstd",
            compression_level=22,
            use_dictionary=True,
            write_statistics=True,
            version="2.6",
            coerce_timestamps="ms",
            allow_truncated_timestamps=True,
            data_page_size=1048576,
            dictionary_pagesize_limit=1048576,
        )

        logger.info(f"Updated main parquet file with new data")
        file_size_mb = os.path.getsize(parquet_file) / (1024 * 1024)
        logger.info(f"New file size: {file_size_mb:.2f} MB")

        # Clean up to save memory
        del existing_df, combined_df, combined_table, processed_df
        gc.collect()

    except FileNotFoundError:
        # If the main file doesn't exist yet, just rename the new file
        logger.info(f"Main parquet file not found. Using new file as main file.")
        os.rename(new_file, parquet_file)

    # Update dictionary files
    update_dictionary_files(updated_dicts)

    return True


def update_dictionary_files(updated_dicts):
    """Update the mapping dictionary files."""
    logger.info("Updating dictionary mapping files...")

    dict_path = "data/dictionaries"
    os.makedirs(dict_path, exist_ok=True)

    for dict_name, dict_data in updated_dicts.items():
        dict_file = os.path.join(dict_path, f"{dict_name}.json")
        with open(dict_file, "w") as f:
            json.dump(dict_data, f, indent=2)

    logger.info("Dictionary files updated successfully")


def main():
    """Main function to update the parquet dataset."""
    logger.info("Starting parquet data update")

    # Ensure data directories exist
    os.makedirs("data", exist_ok=True)
    os.makedirs("data/dictionaries", exist_ok=True)

    # Fetch new data
    new_df, time_str = fetch_new_data()

    if new_df is None:
        logger.error("Failed to fetch new data. Exiting.")
        return

    logger.info(f"Retrieved {len(new_df)} records with timestamp {time_str}")

    # Update parquet file
    success = update_parquet_file(new_df, time_str)

    if success:
        logger.info("Parquet data update completed successfully")
    else:
        logger.error("Failed to update parquet data")

    return success


if __name__ == "__main__":
    main()

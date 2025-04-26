#!/usr/bin/env python3
"""
CSV to Parquet Converter for RedFlagProfits

This script converts the Forbes billionaire dataset from CSV format to optimized Parquet format,
with special handling for nested data structures. Maximum compression is prioritized.

Usage:
    python csv_to_parquet.py

Dependencies:
    pandas, pyarrow, zstandard
"""

import os
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import json
import ast
from glob import glob
from datetime import datetime
import logging
from tqdm import tqdm
import gc  # For garbage collection to manage memory

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("conversion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define common data structures for normalization
COMMON_EXCHANGES = {
    'NYSE': 0,
    'NASDAQ': 1,
    'EURONEXT PARIS': 2,
    'LONDON': 3,
    'TOKYO': 4,
    'HONG KONG': 5,
    'EURONEXT AMSTERDAM': 6,
    'SIX SWISS': 7,
    'MADRID': 8,
    'MEXICO': 9,
    # Additional exchanges will be dynamically added
}

COMMON_CURRENCIES = {
    'USD': 0,
    'EUR': 1,
    'GBP': 2,
    'JPY': 3,
    'HKD': 4,
    'CHF': 5,
    'MXN': 6,
    # Additional currencies will be dynamically added
}

COMMON_INDUSTRIES = {}
COMMON_COMPANIES = {}

def normalize_string_dict(value_dict, reference_dict):
    """
    Replace string values with integer codes for more efficient storage.
    Dynamically adds new values to the reference dictionary.
    
    Args:
        value_dict: Dictionary containing string values to normalize
        reference_dict: Dictionary mapping strings to integer codes
        
    Returns:
        Updated dictionary with integer codes instead of strings
    """
    next_id = max(reference_dict.values()) + 1 if reference_dict else 0
    
    for key, value in list(value_dict.items()):
        if isinstance(value, str) and value not in reference_dict:
            reference_dict[value] = next_id
            next_id += 1
            
    return {k: (reference_dict.get(v, v) if isinstance(v, str) else v) 
            for k, v in value_dict.items()}

def parse_complex_field(field_value):
    """
    Parse complex nested fields from string to proper Python structures.
    Works with both list and dictionary formats.
    
    Args:
        field_value: String representation of a complex data structure
        
    Returns:
        Parsed Python object (list or dict)
    """
    if pd.isna(field_value) or field_value == '':
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

def process_financial_assets(assets_list):
    """
    Process financial assets to optimize storage in parquet.
    
    This function:
    1. Normalizes exchange and currency names to integer codes
    2. Separates data by property for columnar storage efficiency
    3. Handles missing values consistently
    
    Args:
        assets_list: List of financial asset dictionaries
        
    Returns:
        Dictionary with normalized and optimized data structure
    """
    if not assets_list or not isinstance(assets_list, list):
        return {
            "exchanges": [],
            "tickers": [],
            "company_names": [],
            "shares": [],
            "prices": [],
            "currencies": [],
            "exchange_rates": []
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
        exchange = asset.get('exchange', '')
        if exchange not in COMMON_EXCHANGES and exchange:
            COMMON_EXCHANGES[exchange] = len(COMMON_EXCHANGES)
        exchanges.append(COMMON_EXCHANGES.get(exchange, -1))
        
        # Store ticker
        tickers.append(asset.get('ticker', ''))
        
        # Normalize company name
        company = asset.get('companyName', '')
        if company not in COMMON_COMPANIES and company:
            COMMON_COMPANIES[company] = len(COMMON_COMPANIES)
        company_names.append(COMMON_COMPANIES.get(company, -1))
        
        # Extract numerical data
        shares.append(float(asset.get('numberOfShares', 0) or 0))
        prices.append(float(asset.get('sharePrice', 0) or 0))
        
        # Normalize currency
        currency = asset.get('currencyCode', '')
        if currency not in COMMON_CURRENCIES and currency:
            COMMON_CURRENCIES[currency] = len(COMMON_CURRENCIES)
        currencies.append(COMMON_CURRENCIES.get(currency, -1))
        
        # Store exchange rate
        exchange_rates.append(float(asset.get('exchangeRate', 1.0) or 1.0))
    
    return {
        "exchanges": exchanges,
        "tickers": tickers,
        "company_names": company_names,
        "shares": shares,
        "prices": prices,
        "currencies": currencies,
        "exchange_rates": exchange_rates
    }

def process_industries(industries_list):
    """
    Process industries list to optimize storage in parquet.
    
    Args:
        industries_list: List of industry strings
        
    Returns:
        List of industry codes
    """
    if not industries_list or not isinstance(industries_list, list):
        return []
    
    result = []
    for industry in industries_list:
        if industry not in COMMON_INDUSTRIES and industry:
            COMMON_INDUSTRIES[industry] = len(COMMON_INDUSTRIES)
        result.append(COMMON_INDUSTRIES.get(industry, -1))
    
    return result

def process_dataframe(df):
    """
    Process a DataFrame to prepare it for optimized Parquet storage.
    
    Args:
        df: Pandas DataFrame from CSV file
        
    Returns:
        Processed DataFrame ready for Parquet conversion
    """
    # Process complex columns first
    if 'financialAssets' in df.columns:
        # Parse string representations to Python objects
        logger.info("Processing financial assets column...")
        df['financialAssets'] = df['financialAssets'].apply(parse_complex_field)
        
        # Create optimized structure
        assets_data = df['financialAssets'].apply(process_financial_assets)
        
        # Extract columns for columnar storage
        df['asset_exchanges'] = assets_data.apply(lambda x: x['exchanges'])
        df['asset_tickers'] = assets_data.apply(lambda x: x['tickers'])
        df['asset_companies'] = assets_data.apply(lambda x: x['company_names'])
        df['asset_shares'] = assets_data.apply(lambda x: x['shares'])
        df['asset_prices'] = assets_data.apply(lambda x: x['prices'])
        df['asset_currencies'] = assets_data.apply(lambda x: x['currencies'])
        df['asset_exchange_rates'] = assets_data.apply(lambda x: x['exchange_rates'])
        
        # Remove original column to save space
        df = df.drop('financialAssets', axis=1)
    
    # Process industries column
    if 'industries' in df.columns:
        logger.info("Processing industries column...")
        df['industries'] = df['industries'].apply(parse_complex_field)
        df['industry_codes'] = df['industries'].apply(process_industries)
        df = df.drop('industries', axis=1)
    
    # Convert birthDate to proper datetime
    if 'birthDate' in df.columns:
        logger.info("Converting birthDate to datetime...")
        df['birthDate'] = pd.to_datetime(df['birthDate'], unit='ms', errors='coerce')
    
    # Normalize gender to integer
    if 'gender' in df.columns:
        logger.info("Normalizing gender...")
        gender_map = {'M': 0, 'F': 1}
        df['gender'] = df['gender'].map(lambda x: gender_map.get(x, -1))
    
    # Normalize country and source
    if 'countryOfCitizenship' in df.columns:
        countries = {}
        df['country_code'] = df['countryOfCitizenship'].apply(
            lambda x: countries.setdefault(x, len(countries)) if x else -1
        )
        df = df.drop('countryOfCitizenship', axis=1)
    
    if 'source' in df.columns:
        sources = {}
        df['source_code'] = df['source'].apply(
            lambda x: sources.setdefault(x, len(sources)) if x else -1
        )
        df = df.drop('source', axis=1)
    
    return df

def save_optimized_parquet(df, output_file):
    """
    Save DataFrame to Parquet with maximum compression.
    
    Args:
        df: Processed DataFrame
        output_file: Path to save the Parquet file
    """
    logger.info(f"Saving optimized parquet to {output_file}...")
    
    # Convert to PyArrow Table
    table = pa.Table.from_pandas(df)
    
    # Write to parquet with maximum compression
    pq.write_table(
        table,
        output_file,
        compression="zstd",  # zstd provides better compression than snappy
        compression_level=22,  # Maximum compression level
        use_dictionary=True,
        write_statistics=True,
        version="2.6",  # Use latest version for best features
        data_page_size=1048576,  # 1MB pages for better compression
        dictionary_pagesize_limit=1048576  # 1MB dictionary pages
    )
    
    # Check compression ratio
    parquet_size = os.path.getsize(output_file)
    logger.info(f"Compressed size: {parquet_size / (1024 * 1024):.2f} MB")

def create_combined_dictionary_files():
    """
    Save mapping dictionaries as JSON files for reference and later use.
    """
    logger.info("Saving dictionary mapping files...")
    os.makedirs("data_parquet/dictionaries", exist_ok=True)
    
    # Save mapping dictionaries
    with open("data_parquet/dictionaries/exchanges.json", "w") as f:
        json.dump(COMMON_EXCHANGES, f, indent=2)
    
    with open("data_parquet/dictionaries/currencies.json", "w") as f:
        json.dump(COMMON_CURRENCIES, f, indent=2)
    
    with open("data_parquet/dictionaries/industries.json", "w") as f:
        json.dump(COMMON_INDUSTRIES, f, indent=2)
    
    with open("data_parquet/dictionaries/companies.json", "w") as f:
        json.dump(COMMON_COMPANIES, f, indent=2)

def create_combined_parquet():
    """
    Create a single merged parquet file with all billionaire data.
    Optimized for maximum compression and storage efficiency.
    """
    logger.info("Creating single combined parquet file...")
    parquet_files = sorted(glob("data_parquet/*.parquet"))
    if not parquet_files:
        logger.warning("No parquet files found to combine")
        return
    
    output_file = "data_parquet/all_billionaires.parquet"
    
    # Initialize a list to collect schema information
    schema = None
    
    # Process files in batches to avoid memory issues
    batch_size = 10
    all_dfs = []
    
    for i in range(0, len(parquet_files), batch_size):
        batch_files = parquet_files[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1} of {(len(parquet_files) + batch_size - 1) // batch_size}")
        
        # Read batch of parquet files
        batch_dfs = []
        for file in batch_files:
            logger.info(f"Reading {file}...")
            df = pd.read_parquet(file)
            
            # Collect schema from first file to ensure consistency
            if schema is None:
                schema = pa.Table.from_pandas(df.head(0)).schema
                
            # Add temporal info for potential filtering later
            df['year'] = df['crawl_date'].dt.year
            df['month'] = df['crawl_date'].dt.month
            df['day'] = df['crawl_date'].dt.day
            
            batch_dfs.append(df)
        
        # Combine batch
        batch_df = pd.concat(batch_dfs, ignore_index=True)
        all_dfs.append(batch_df)
        
        # Clean up batch data to save memory
        del batch_dfs
        gc.collect()
    
    # Combine all batches
    logger.info("Merging all batches...")
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Sort by date for better compression
    logger.info("Sorting data by date...")
    combined_df.sort_values(by=['year', 'month', 'day', 'personName'], inplace=True)
    
    # Convert to Arrow table
    logger.info("Converting to Arrow table...")
    combined_table = pa.Table.from_pandas(combined_df)
    
    # Save as single parquet file with maximum compression
    logger.info(f"Writing combined file to {output_file}...")
    pq.write_table(
        combined_table,
        output_file,
        compression="zstd",
        compression_level=22,  # Maximum zstd compression level
        use_dictionary=True,
        write_statistics=True,
        version="2.6",
        coerce_timestamps="ms",
        allow_truncated_timestamps=True,
        data_page_size=1048576,  # 1MB pages for better compression
        dictionary_pagesize_limit=1048576  # 1MB dictionary pages
    )
    
    # Report file size
    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    logger.info(f"Combined file created successfully: {file_size_mb:.2f} MB")
    
    # Clean up to save memory
    del combined_df, combined_table, all_dfs
    gc.collect()

def convert_csv_to_parquet():
    """
    Main function to convert all CSV files to optimized Parquet format.
    """
    # Get all CSV files in the data directory
    csv_files = sorted(glob("data/*.csv"))
    if not csv_files:
        logger.error("No CSV files found in data directory")
        return
    
    logger.info(f"Found {len(csv_files)} CSV files to convert")
    
    # Create directory for parquet files if it doesn't exist
    os.makedirs("data_parquet", exist_ok=True)
    
    # Process each file
    for csv_file in tqdm(csv_files, desc="Converting files"):
        # Extract timestamp from filename
        file_date = os.path.basename(csv_file).split('.')[0]
        
        # Define output file
        output_file = f"data_parquet/{file_date}.parquet"
        
        # Skip if already processed
        if os.path.exists(output_file) and not os.environ.get('FORCE_RECONVERT'):
            logger.info(f"Skipping {csv_file} (already converted)")
            continue
        
        logger.info(f"Processing {csv_file}...")
        
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file)
            
            # Add crawl date
            df['crawl_date'] = pd.to_datetime(file_date)
            
            # Process the DataFrame
            df = process_dataframe(df)
            
            # Save to parquet
            save_optimized_parquet(df, output_file)
            
            # Clean up to save memory
            del df
            gc.collect()
            
        except Exception as e:
            logger.error(f"Error processing {csv_file}: {e}")
    
    # Save mapping dictionaries
    create_combined_dictionary_files()
    
    # Create combined dataset
    create_combined_parquet()
    
    logger.info("Conversion complete!")

if __name__ == "__main__":
    logger.info("Starting CSV to Parquet conversion process")
    convert_csv_to_parquet()   convert_csv_to_parquet()

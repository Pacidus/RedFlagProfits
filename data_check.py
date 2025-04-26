#!/usr/bin/env python3
"""
Parquet Data Testing Script for RedFlagProfits

This script tests reading from the parquet file and creates a test visualization
to verify data integrity after conversion.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import os
import json
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Paths
PARQUET_FILE = "data_parquet/all_billionaires.parquet"
DICT_PATH = "data_parquet/dictionaries"


def load_dictionaries():
    """Load the mapping dictionaries used during conversion."""
    dicts = {}

    for dict_name in ["exchanges", "currencies", "industries", "companies"]:
        dict_file = os.path.join(DICT_PATH, f"{dict_name}.json")
        if os.path.exists(dict_file):
            with open(dict_file, "r") as f:
                dicts[dict_name] = json.load(f)
                # Create reverse mapping for easier lookup
                dicts[f"{dict_name}_rev"] = {v: k for k, v in dicts[dict_name].items()}

    return dicts


def read_parquet_data():
    """Read the parquet file and return the data."""
    logger.info(f"Reading parquet file: {PARQUET_FILE}")

    if not os.path.exists(PARQUET_FILE):
        logger.error(f"Parquet file not found: {PARQUET_FILE}")
        return None

    # Read file size
    file_size_mb = os.path.getsize(PARQUET_FILE) / (1024 * 1024)
    logger.info(f"Parquet file size: {file_size_mb:.2f} MB")

    # Read the data
    start_time = datetime.now()
    df = pd.read_parquet(PARQUET_FILE)
    end_time = datetime.now()

    logger.info(
        f"Read {len(df)} rows in {(end_time - start_time).total_seconds():.2f} seconds"
    )
    logger.info(
        f"DataFrame memory usage: {df.memory_usage(deep=True).sum() / (1024 * 1024):.2f} MB"
    )

    return df


def process_data_for_graph(df, min_worth=0):
    """
    Process the data similar to your original process_file_lazy function.

    Args:
        df: DataFrame with billionaire data
        min_worth: Minimum worth threshold, defaults to 0

    Returns:
        Dictionary with processed data
    """
    # Group by date and calculate aggregates
    logger.info("Processing data for graphs...")

    # Filter by minimum worth
    filtered_df = df[
        (df["finalWorth"] >= min_worth)
        & (df["estWorthPrev"] >= min_worth)
        & (df["archivedWorth"] >= min_worth)
        & (df["privateAssetsWorth"] >= min_worth)
    ]

    # Group by crawl_date and calculate metrics
    grouped = (
        filtered_df.groupby("crawl_date")
        .agg(
            N_Bi=("finalWorth", "count"),
            totF=("finalWorth", lambda x: x.sum() / 1e3),
            totB=("estWorthPrev", lambda x: x.sum() / 1e3),
            totA=("archivedWorth", lambda x: x.sum() / 1e3),
            totP=("privateAssetsWorth", lambda x: x.sum() / 1e3),
        )
        .reset_index()
    )

    # Sort by date
    grouped = grouped.sort_values("crawl_date")

    return grouped


def create_test_plot(data_df, title, filename):
    """
    Create a simple test plot to verify the data.

    Args:
        data_df: DataFrame with processed data
        title: Plot title
        filename: Output file name
    """
    logger.info(f"Creating test plot: {title}")

    plt.figure(figsize=(10, 6))

    # Plot wealth metrics
    colors = ["slategray", "lightsteelblue", "mediumseagreen", "darkorchid"]
    labels = ["Archived Worth", "Final Worth", "Begin Worth", "Private Assets"]
    columns = ["totA", "totF", "totB", "totP"]

    for col, color, label in zip(columns, colors, labels):
        plt.plot(
            data_df["crawl_date"],
            data_df[col],
            marker=".",
            linestyle="None",
            color=color,
            label=label,
        )

    # Configure plot
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("Billions of Dollars")
    plt.xticks(rotation=-30)
    plt.legend()
    plt.tight_layout()

    # Save plot
    plt.savefig(filename)
    logger.info(f"Plot saved to {filename}")

    # Create second plot showing count
    plt.figure(figsize=(10, 3))
    plt.plot(data_df["crawl_date"], data_df["N_Bi"], color="blue")
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.title(f"Number of {title}")
    plt.xlabel("Date")
    plt.ylabel("Count")
    plt.xticks(rotation=-30)
    plt.tight_layout()

    # Save count plot
    count_filename = filename.replace(".png", "_count.png")
    plt.savefig(count_filename)
    logger.info(f"Count plot saved to {count_filename}")


def main():
    """Main function to test parquet data and create visualizations."""
    logger.info("Starting parquet data test")

    # Load dictionaries
    dictionaries = load_dictionaries()
    logger.info(f"Loaded {len(dictionaries)} dictionaries")

    # Read parquet data
    df = read_parquet_data()
    if df is None:
        return

    # Display a data sample
    logger.info("\nData sample:")
    logger.info(df.head(3))

    # Basic data validation
    logger.info("\nData validation:")
    logger.info(f"Date range: {df['crawl_date'].min()} to {df['crawl_date'].max()}")
    logger.info(f"Number of unique dates: {df['crawl_date'].nunique()}")
    logger.info(f"Number of unique billionaires: {df['personName'].nunique()}")

    # Process data for billionaires (worth >= 1B)
    billionaires_df = process_data_for_graph(df, min_worth=1000)
    logger.info(f"Processed data for billionaires: {len(billionaires_df)} data points")

    # Process data for millionaires (worth >= 1M)
    millionaires_df = process_data_for_graph(df, min_worth=1)
    logger.info(f"Processed data for millionaires: {len(millionaires_df)} data points")

    # Create test plots
    create_test_plot(billionaires_df, "Billionaires (≥ 1B)", "billionaires_test.png")
    create_test_plot(millionaires_df, "Millionaires (≥ 1M)", "millionaires_test.png")

    logger.info("Parquet data test completed successfully")


if __name__ == "__main__":
    main()

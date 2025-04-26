#!/usr/bin/env python3
"""
Parquet Data Exploration Tool for RedFlagProfits

This script provides utilities for exploring and analyzing the parquet dataset.
"""

import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import argparse
import os
import json
import matplotlib.pyplot as plt
from datetime import datetime


def load_dictionaries():
    """Load mapping dictionaries for decoding."""
    dicts = {}
    dict_path = "data/dictionaries"

    for dict_name in ["exchanges", "currencies", "industries", "companies"]:
        dict_file = os.path.join(dict_path, f"{dict_name}.json")
        if os.path.exists(dict_file):
            with open(dict_file, "r") as f:
                dicts[dict_name] = json.load(f)
                # Create reverse mapping
                dicts[f"{dict_name}_rev"] = {
                    int(v): k for k, v in dicts[dict_name].items()
                }

    return dicts


def decode_asset_data(row, dictionaries):
    """Decode asset data back to human-readable format."""
    if "asset_tickers" not in row or len(row["asset_tickers"]) == 0:
        return []

    assets = []
    exchanges_rev = dictionaries.get("exchanges_rev", {})
    companies_rev = dictionaries.get("companies_rev", {})
    currencies_rev = dictionaries.get("currencies_rev", {})

    for i in range(len(row["asset_tickers"])):
        asset = {}

        # Get exchange name
        if "asset_exchanges" in row and i < len(row["asset_exchanges"]):
            exchange_code = row["asset_exchanges"][i]
            asset["exchange"] = exchanges_rev.get(
                exchange_code, f"Unknown_{exchange_code}"
            )

        # Get ticker
        if i < len(row["asset_tickers"]):
            asset["ticker"] = row["asset_tickers"][i]

        # Get company name
        if "asset_companies" in row and i < len(row["asset_companies"]):
            company_code = row["asset_companies"][i]
            asset["companyName"] = companies_rev.get(
                company_code, f"Unknown_{company_code}"
            )

        # Get shares and price
        if "asset_shares" in row and i < len(row["asset_shares"]):
            asset["numberOfShares"] = row["asset_shares"][i]
        if "asset_prices" in row and i < len(row["asset_prices"]):
            asset["sharePrice"] = row["asset_prices"][i]

        # Get currency and exchange rate
        if "asset_currencies" in row and i < len(row["asset_currencies"]):
            currency_code = row["asset_currencies"][i]
            asset["currencyCode"] = currencies_rev.get(
                currency_code, f"Unknown_{currency_code}"
            )
        if "asset_exchange_rates" in row and i < len(row["asset_exchange_rates"]):
            asset["exchangeRate"] = row["asset_exchange_rates"][i]

        assets.append(asset)

    return assets


def decode_industries(row, dictionaries):
    """Decode industry codes back to human-readable format."""
    if "industry_codes" not in row or len(row["industry_codes"]) == 0:
        return []

    industries_rev = dictionaries.get("industries_rev", {})
    return [
        industries_rev.get(code, f"Unknown_{code}") for code in row["industry_codes"]
    ]


def analyze_parquet_file(file_path, verbose=False):
    """Analyze the structure of a parquet file."""
    print(f"Analyzing parquet file: {file_path}")

    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File not found - {file_path}")
        return

    # File info
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"File size: {file_size_mb:.2f} MB")

    # Read parquet metadata
    parquet_file = pq.ParquetFile(file_path)
    num_row_groups = parquet_file.num_row_groups
    metadata = parquet_file.metadata

    print(f"Number of row groups: {num_row_groups}")
    print(f"Total rows: {metadata.num_rows}")
    print(f"Number of columns: {metadata.num_columns}")

    # Print schema
    print("\nSchema:")
    print(parquet_file.schema_arrow)

    if verbose:
        # Print row group details
        print("\nRow group details:")
        for i in range(num_row_groups):
            rg = metadata.row_group(i)
            print(
                f"  Row group {i}: {rg.num_rows} rows, {rg.total_byte_size / 1024 / 1024:.2f} MB"
            )

    # Read a sample of the data
    df = pd.read_parquet(file_path, engine="pyarrow")
    print(f"\nSample data ({min(5, len(df))} rows):")
    print(df.head(5))

    # Data summary
    print("\nData summary:")
    print(f"Date range: {df['crawl_date'].min()} to {df['crawl_date'].max()}")
    print(f"Number of unique dates: {df['crawl_date'].nunique()}")
    print(f"Number of billionaires: {df['personName'].nunique()}")

    # Memory usage
    memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024
    print(f"DataFrame memory usage: {memory_usage:.2f} MB")

    return df


def show_billionaire_details(df, name, dictionaries=None):
    """Show detailed information for a specific billionaire."""
    if dictionaries is None:
        dictionaries = load_dictionaries()

    # Find the billionaire
    matches = df[df["personName"].str.contains(name, case=False, na=False)]

    if len(matches) == 0:
        print(f"No billionaire found matching: {name}")
        return

    print(f"Found {len(matches)} records for billionaires matching '{name}'")

    # Get the most recent record
    latest = matches.loc[matches["crawl_date"].idxmax()]

    print(f"\nLatest record for: {latest['personName']}")
    print(f"Date: {latest['crawl_date']}")
    print(f"Final Worth: ${latest['finalWorth']:.2f} million")
    print(f"Previous Worth: ${latest['estWorthPrev']:.2f} million")
    print(f"Private Assets Worth: ${latest['privateAssetsWorth']:.2f} million")
    print(f"Archived Worth: ${latest['archivedWorth']:.2f} million")

    # Decode and display complex fields
    industries = decode_industries(latest, dictionaries)
    print(f"Industries: {', '.join(industries)}")

    # Display assets
    assets = decode_asset_data(latest, dictionaries)
    if assets:
        print("\nFinancial Assets:")
        for i, asset in enumerate(assets):
            print(f"  Asset {i+1}:")
            for key, value in asset.items():
                print(f"    {key}: {value}")

            # Plot worth over time if multiple records exist
    if len(matches) > 1:
        print("\nGenerating worth history plot...")
        plt.figure(figsize=(10, 6))

        sorted_matches = matches.sort_values("crawl_date")
        plt.plot(
            sorted_matches["crawl_date"],
            sorted_matches["finalWorth"],
            "o-",
            label="Final Worth",
        )
        plt.plot(
            sorted_matches["crawl_date"],
            sorted_matches["estWorthPrev"],
            "s--",
            label="Previous Worth",
        )
        plt.plot(
            sorted_matches["crawl_date"],
            sorted_matches["privateAssetsWorth"],
            "^-.",
            label="Private Assets",
        )
        plt.plot(
            sorted_matches["crawl_date"],
            sorted_matches["archivedWorth"],
            "d:",
            label="Archived Worth",
        )

        plt.title(f"Worth History: {latest['personName']}")
        plt.xlabel("Date")
        plt.ylabel("Million USD")
        plt.grid(True, linestyle="--", alpha=0.7)
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

    return matches


def find_top_billionaires(df, date=None, count=10):
    """Find the top billionaires by worth for a specific date or the most recent date."""
    if date is None:
        # Use the most recent date
        date = df["crawl_date"].max()
    else:
        # Convert string to datetime if needed
        if isinstance(date, str):
            date = pd.to_datetime(date)

        # Find the closest available date
        available_dates = sorted(df["crawl_date"].unique())
        closest_date = min(available_dates, key=lambda x: abs(x - date))

        if closest_date != date:
            print(f"Date {date} not found, using closest date: {closest_date}")
            date = closest_date

    # Filter data for the selected date
    date_df = df[df["crawl_date"] == date]

    # Sort by final worth and get top entries
    top_billionaires = date_df.sort_values("finalWorth", ascending=False).head(count)

    print(f"\nTop {count} billionaires as of {date}:")
    print(f"{'Rank':<5}{'Name':<40}{'Worth ($M)':<15}{'Industries':<30}")
    print("-" * 90)

    # Load dictionaries for decoding industries
    dictionaries = load_dictionaries()

    for i, (_, row) in enumerate(top_billionaires.iterrows()):
        industries = decode_industries(row, dictionaries)
        industry_str = ", ".join(industries[:2])
        if len(industries) > 2:
            industry_str += "..."

        print(
            f"{i+1:<5}{row['personName']:<40}{row['finalWorth']:<15,.2f}{industry_str:<30}"
        )

    return top_billionaires


def plot_wealth_distribution(df, date=None, bins=50):
    """Plot the distribution of wealth for a specific date."""
    if date is None:
        # Use the most recent date
        date = df["crawl_date"].max()
    else:
        # Convert string to datetime if needed
        if isinstance(date, str):
            date = pd.to_datetime(date)

        # Find the closest available date
        available_dates = sorted(df["crawl_date"].unique())
        closest_date = min(available_dates, key=lambda x: abs(x - date))

        if closest_date != date:
            print(f"Date {date} not found, using closest date: {closest_date}")
            date = closest_date

    # Filter data for the selected date
    date_df = df[df["crawl_date"] == date]

    # Create plots
    fig, axes = plt.subplots(2, 1, figsize=(12, 10))

    # Linear scale histogram
    axes[0].hist(date_df["finalWorth"], bins=bins, alpha=0.7, color="blue")
    axes[0].set_title(f"Wealth Distribution (Linear Scale) - {date}")
    axes[0].set_xlabel("Worth (Million USD)")
    axes[0].set_ylabel("Count")
    axes[0].grid(True, linestyle="--", alpha=0.7)

    # Log scale histogram
    axes[1].hist(
        date_df["finalWorth"],
        bins=np.logspace(
            np.log10(date_df["finalWorth"].min()),
            np.log10(date_df["finalWorth"].max()),
            bins,
        ),
        alpha=0.7,
        color="green",
    )
    axes[1].set_title(f"Wealth Distribution (Log Scale) - {date}")
    axes[1].set_xlabel("Worth (Million USD)")
    axes[1].set_ylabel("Count")
    axes[1].set_xscale("log")
    axes[1].grid(True, linestyle="--", alpha=0.7)

    plt.tight_layout()
    plt.show()

    # Additional statistics
    print(f"\nWealth statistics as of {date}:")
    print(f"Total billionaires: {len(date_df)}")
    print(f"Total wealth: ${date_df['finalWorth'].sum() / 1000:.2f} billion")
    print(f"Mean wealth: ${date_df['finalWorth'].mean():.2f} million")
    print(f"Median wealth: ${date_df['finalWorth'].median():.2f} million")
    print(f"Minimum wealth: ${date_df['finalWorth'].min():.2f} million")
    print(f"Maximum wealth: ${date_df['finalWorth'].max():.2f} million")

    return date_df


def analyze_industry_distribution(df, date=None):
    """Analyze the distribution of billionaires by industry."""
    if date is None:
        # Use the most recent date
        date = df["crawl_date"].max()
    else:
        # Convert string to datetime if needed
        if isinstance(date, str):
            date = pd.to_datetime(date)

        # Find the closest available date
        available_dates = sorted(df["crawl_date"].unique())
        closest_date = min(available_dates, key=lambda x: abs(x - date))

        if closest_date != date:
            print(f"Date {date} not found, using closest date: {closest_date}")
            date = closest_date

    # Filter data for the selected date
    date_df = df[df["crawl_date"] == date]

    # Load dictionaries for decoding
    dictionaries = load_dictionaries()
    industries_rev = dictionaries.get("industries_rev", {})

    # Extract all industries
    all_industries = []
    industry_wealth = {}
    industry_count = {}

    for _, row in date_df.iterrows():
        if "industry_codes" in row and isinstance(row["industry_codes"], list):
            industries = [
                industries_rev.get(code, f"Unknown_{code}")
                for code in row["industry_codes"]
            ]

            # Add to overall list
            all_industries.extend(industries)

            # Add to counts and wealth
            worth = row["finalWorth"]
            for industry in industries:
                if industry in industry_count:
                    industry_count[industry] += 1
                    industry_wealth[industry] += worth
                else:
                    industry_count[industry] = 1
                    industry_wealth[industry] = worth

    # Sort by count
    sorted_industries = sorted(industry_count.items(), key=lambda x: x[1], reverse=True)

    # Display results
    print(f"\nIndustry distribution as of {date}:")
    print(
        f"{'Industry':<30}{'Count':<10}{'Total Wealth ($B)':<20}{'Avg Wealth ($M)':<20}"
    )
    print("-" * 80)

    for industry, count in sorted_industries[:20]:  # Show top 20
        total_wealth = industry_wealth[industry] / 1000  # Convert to billions
        avg_wealth = industry_wealth[industry] / count
        print(f"{industry:<30}{count:<10}{total_wealth:<20,.2f}{avg_wealth:<20,.2f}")

    # Plot industry distribution (top 10)
    top_industries = dict(sorted_industries[:10])

    plt.figure(figsize=(12, 8))
    plt.barh(
        list(top_industries.keys()), list(top_industries.values()), color="skyblue"
    )
    plt.xlabel("Number of Billionaires")
    plt.ylabel("Industry")
    plt.title(f"Top 10 Industries by Billionaire Count - {date}")
    plt.grid(True, linestyle="--", alpha=0.7, axis="x")
    plt.tight_layout()
    plt.show()

    # Plot industry wealth distribution (top 10)
    top_wealth_industries = {
        k: industry_wealth[k] / 1000 for k, _ in sorted_industries[:10]
    }  # Convert to billions

    plt.figure(figsize=(12, 8))
    plt.barh(
        list(top_wealth_industries.keys()),
        list(top_wealth_industries.values()),
        color="lightgreen",
    )
    plt.xlabel("Total Wealth (Billion USD)")
    plt.ylabel("Industry")
    plt.title(f"Top 10 Industries by Total Wealth - {date}")
    plt.grid(True, linestyle="--", alpha=0.7, axis="x")
    plt.tight_layout()
    plt.show()

    return sorted_industries


def main():
    """Main function for command-line interface."""
    parser = argparse.ArgumentParser(
        description="Explore parquet data for RedFlagProfits"
    )

    parser.add_argument(
        "--analyze", action="store_true", help="Analyze the parquet file structure"
    )
    parser.add_argument("--verbose", action="store_true", help="Show verbose output")
    parser.add_argument(
        "--billionaire",
        type=str,
        help="Show details for a specific billionaire (substring match)",
    )
    parser.add_argument("--top", type=int, default=10, help="Show top N billionaires")
    parser.add_argument(
        "--date", type=str, help="Date to analyze (YYYY-MM-DD), defaults to most recent"
    )
    parser.add_argument(
        "--distribution", action="store_true", help="Plot wealth distribution"
    )
    parser.add_argument(
        "--industries", action="store_true", help="Analyze industry distribution"
    )
    parser.add_argument(
        "--file",
        type=str,
        default="data/all_billionaires.parquet",
        help="Path to parquet file (default: data/all_billionaires.parquet)",
    )

    args = parser.parse_args()

    # If no arguments are provided, show help
    if len(vars(args)) <= 2 and not (
        args.analyze or args.billionaire or args.distribution or args.industries
    ):
        parser.print_help()
        return

    # Read the parquet file
    if args.analyze:
        df = analyze_parquet_file(args.file, args.verbose)
    else:
        df = pd.read_parquet(args.file)

    # Process date if provided
    date = None
    if args.date:
        date = pd.to_datetime(args.date)

    # Execute requested actions
    if args.billionaire:
        show_billionaire_details(df, args.billionaire)

    if args.top:
        find_top_billionaires(df, date, args.top)

    if args.distribution:
        plot_wealth_distribution(df, date)

    if args.industries:
        analyze_industry_distribution(df, date)


if __name__ == "__main__":
    main()

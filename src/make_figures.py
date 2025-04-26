#!/usr/bin/env python3
"""
Figure generation script for RedFlagProfits using Parquet data

This script processes the Parquet dataset and creates visualizations
using both matplotlib and plotly.
"""

import sys
import numpy as np
import polars as pl
import pandas as pd
from datetime import datetime
from matplotlib_svg import plot_svg
from plotly_html import plot_html
from common_plot_config import unified_params, data_labels, colors


def process_parquet_lazy(parquet_path, min_worth=0):
    """Process Parquet file using Polars lazy API."""
    # Read the parquet file
    df = pl.scan_parquet(parquet_path)

    # Group by date and calculate aggregates
    result = (
        df.filter(
            (pl.col("finalWorth") >= min_worth)
            & (pl.col("estWorthPrev") >= min_worth)
            & (pl.col("archivedWorth") >= min_worth)
            & (pl.col("privateAssetsWorth") >= min_worth)
        )
        .groupby("crawl_date")
        .agg(
            [
                pl.len().alias("N_Bi"),
                (pl.sum("finalWorth") / 1e3).alias("totF"),
                (pl.sum("estWorthPrev") / 1e3).alias("totB"),
                (pl.sum("archivedWorth") / 1e3).alias("totA"),
                (pl.sum("privateAssetsWorth") / 1e3).alias("totP"),
            ]
        )
        .sort("crawl_date")
        .collect()
    )

    return result


def calculate_trendline(numeric_dates, values):
    """Calculate log-linear trendline."""
    # Filter out any zeros or negative values for log calculation
    mask = values > 0
    if sum(mask) <= 1:  # Need at least two points for a trendline
        return numeric_dates, values, float("nan")

    slope, intercept = np.polyfit(numeric_dates[mask], np.log(values[mask]), 1)
    trend_dates = np.linspace(numeric_dates.min(), numeric_dates.max(), 100)
    trend_values = np.exp(slope * trend_dates + intercept)

    # Calculate doubling time in years (assuming numeric_dates in days)
    doubling_time = np.log(2) / slope / 365.25 if slope > 0 else float("inf")

    return trend_dates, trend_values, doubling_time


def main(min_worth, group_label, group_desc, show=False):
    """Main processing and plotting routine."""
    parquet_file = "data/all_billionaires.parquet"
    print(f"Processing {group_label} data (minimum worth: ${min_worth} million)...")

    # Process the parquet file
    results = process_parquet_lazy(parquet_file, min_worth)

    # Extract dates and values
    dates = results["crawl_date"].to_numpy().astype("datetime64[D]")
    individual_counts = results["N_Bi"].to_numpy()

    # Extract wealth metrics
    wealth_columns = [
        results["totA"].to_numpy(),  # Archived Worth
        results["totF"].to_numpy(),  # Final Worth
        results["totB"].to_numpy(),  # Begin Worth
        results["totP"].to_numpy(),  # Private Assets Worth
    ]

    # Precompute trends
    numeric_dates = dates.astype("datetime64[D]").astype(float)
    trend_data = []
    for data in wealth_columns:
        trend_numeric, trend_vals, doubling = calculate_trendline(numeric_dates, data)
        trend_dates = trend_numeric.astype("datetime64[D]")
        trend_data.append((trend_dates, trend_vals, doubling))

    # Generate plots
    print(f"Generating {group_label} plots...")
    plot_svg(
        dates,
        wealth_columns,
        individual_counts,
        trend_data,
        group_label,
        group_desc,
        show,
    )
    plot_html(
        dates,
        wealth_columns,
        individual_counts,
        trend_data,
        group_label,
        group_desc,
    )

    print(f"{group_label} plots generated successfully")

    # Return data for potential further use
    return dates, wealth_columns, individual_counts, trend_data


if __name__ == "__main__":
    display_plot = len(sys.argv) > 1 and sys.argv[1] == "show"

    # Update the output filename paths
    output_dir = "docs/figures"
    import os

    os.makedirs(output_dir, exist_ok=True)

    # Process billionaires (worth >= $1 billion)
    main(1000, f"{output_dir}/Billionaires", r"Billionaires ($\geq$ 1B)", display_plot)

    # Process millionaires (worth >= $1 million)
    main(1, f"{output_dir}/Millionaires", r"Millionaires ($\geq$ 1M)", display_plot)

    # Update the README last run timestamp
    with open("README.md", "r") as f:
        readme_content = f.read()

    # Replace the last line with current timestamp
    current_time = datetime.utcnow().strftime("%a %b %d %H:%M:%S UTC %Y")
    readme_lines = readme_content.split("\n")
    if "Last run:" in readme_lines[-1]:
        readme_lines[-1] = f"Last run: {current_time}"
    else:
        readme_lines.append(f"Last run: {current_time}")

    with open("README.md", "w") as f:
        f.write("\n".join(readme_lines))

    print(f"README updated with timestamp: {current_time}")

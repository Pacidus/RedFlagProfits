import sys
import numpy as np
import polars as pl
from glob import glob
from matplotlib_svg import plot_svg
from plotly_html import plot_html
from common_plot_config import unified_params, data_labels, colors


def process_file_lazy(file_path, min_worth=0):
    """Process CSV file using Polars lazy API."""
    return (
        pl.scan_csv(file_path)
        .filter(
            (pl.col("finalWorth") >= min_worth)
            & (pl.col("estWorthPrev") >= min_worth)
            & (pl.col("archivedWorth") >= min_worth)
            & (pl.col("privateAssetsWorth") >= min_worth)
        )
        .select(
            [
                pl.len().alias("N_Bi"),
                pl.sum("finalWorth").alias("totF") / 1e3,
                pl.sum("estWorthPrev").alias("totB") / 1e3,
                pl.sum("archivedWorth").alias("totA") / 1e3,
                pl.sum("privateAssetsWorth").alias("totP") / 1e3,
            ]
        )
        .collect()[0]
    )


def calculate_trendline(numeric_dates, values):
    """Calculate log-linear trendline."""
    slope, intercept = np.polyfit(numeric_dates, np.log(values), 1)
    trend_dates = np.linspace(numeric_dates.min(), numeric_dates.max(), 100)
    trend_values = np.exp(slope * trend_dates + intercept)
    doubling_time = np.log(2) / slope / 365.25
    return trend_dates, trend_values, doubling_time


def main(min_worth, group_label, group_desc, show):
    """Main processing and plotting routine."""
    data_files = glob("data/*.csv")
    dates = np.array(
        [path[5:].split(".")[0] for path in data_files], dtype="datetime64[D]"
    )
    sorted_indices = np.argsort(dates)
    dates = dates[sorted_indices]

    # Initialize data containers
    individual_counts = np.zeros_like(dates, dtype=float)
    wealth_columns = [np.zeros_like(dates, dtype=float) for _ in range(4)]

    # Process files
    for i, idx in enumerate(sorted_indices):
        result = process_file_lazy(data_files[idx], min_worth)
        individual_counts[i] = result["N_Bi"].item()
        wealth_columns[0][i] = result["totA"].item()
        wealth_columns[1][i] = result["totF"].item()
        wealth_columns[2][i] = result["totB"].item()
        wealth_columns[3][i] = result["totP"].item()

    # Precompute trends
    numeric_dates = dates.astype("datetime64[D]").astype("float")
    trend_data = []
    for data in wealth_columns:
        trend_numeric, trend_vals, doubling = calculate_trendline(numeric_dates, data)
        trend_dates = trend_numeric.astype("datetime64[D]")
        trend_data.append((trend_dates, trend_vals, doubling))

    # Generate plots
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


if __name__ == "__main__":
    display_plot = len(sys.argv) > 1 and sys.argv[1] == "show"
    main(1000, "Billionaires", r"Billionaires ($\geq$ 1B)", display_plot)
    main(1, "Millionaires", r"Millionaires ($\geq$ 1M)", display_plot)

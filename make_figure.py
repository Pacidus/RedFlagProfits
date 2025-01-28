import sys
import numpy as np
import polars as pl
import matplotlib.pyplot as plt

from glob import glob

def process_file_lazy(path, min_worth=0):
    """
    Process the CSV file and return the aggregated financial data.

    Args:
    - path: Path to the CSV file
    - min_worth: Minimum worth threshold for filtering

    Returns:
    - A dictionary containing the aggregated values
    """
    totals = pl.scan_csv(
        path,
        schema_overrides={
            "finalWorth": pl.Float64,
            "estWorthPrev": pl.Float64,
            "archivedWorth": pl.Float64,
            "privateAssetsWorth": pl.Float64,
        },
    ).filter(
        (pl.col("finalWorth") >= min_worth)
        & (pl.col("estWorthPrev") >= min_worth)
        & (pl.col("archivedWorth") >= min_worth)
        & (pl.col("privateAssetsWorth") >= min_worth)
    ).select([
        pl.len().alias("N_Bi"),
        pl.sum("finalWorth").alias("totF") / 1e3,
        pl.sum("estWorthPrev").alias("totB") / 1e3,
        pl.sum("archivedWorth").alias("totA") / 1e3,
        pl.sum("privateAssetsWorth").alias("totP") / 1e3,
    ]).collect()
    return totals[0]

def fit_trendline(date, data):
    """
    Fit a log-linear trendline to the data.

    Args:
    - date: Array of date values (numeric)
    - data: Array of data values

    Returns:
    - x_date: Interpolated date values for the trendline
    - fitted_data: Fitted data points for the trendline
    - doubling_time_years: Estimated doubling time in years
    """
    date_numeric = date.astype("float")
    slope, intercept = np.polyfit(date_numeric, np.log(data), 1)
    x_date = np.linspace(date_numeric.min(), date_numeric.max(), 100)
    fitted_data = np.exp(slope * x_date + intercept)
    doubling_time_years = np.log(2) / slope / 365.25
    return x_date, fitted_data, doubling_time_years

def plot_data(date, totA, totF, totB, totP, N_Bi, group_label, group_desc, show=True):
    """
    Plot the financial data and sparklines.

    Args:
    - date: Array of dates
    - totA, totF, totB, totP: Financial data arrays
    - N_Bi: Number of entities (e.g., billionaires/millionaires)
    - group_label: Label for the group (e.g., "Billionaires")
    - group_desc: Description of the group
    - show: Whether to display the plot
    """
    plt.rc("text", usetex=True)
    plt.rc("font", family="serif")
    plt.style.use("dark_background")

    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot data points
    colors = ["slategray", "lightsteelblue", "mediumseagreen", "darkorchid"]
    labels = ["Archived Worth", "Final Worth", "Begin Worth", "Private Assets"]
    datasets = [totA, totF, totB, totP]

    for data, color, label in zip(datasets, colors, labels):
        ax.plot(date, data, label=label, color=color, marker=".", linestyle="None")
        x_date, fitted_data, doubling_time_years = fit_trendline(date, data)
        ax.plot(
            x_date,
            fitted_data,
            color=color,
            linestyle="--",
            linewidth=1,
            alpha=0.4,
            label=f"{label} Trend:\nDoubling Time = {doubling_time_years:.2f} years",
        )

    # Customize main plot
    ax.set_ylabel("Billions of Dollars", fontsize=12, color="white")
    ax.set_xlabel("Date", fontsize=12, color="white")
    ax.tick_params(axis="x", labelrotation=30, labelsize=10, color="white")
    ax.tick_params(axis="y", labelsize=10, color="white")
    ax.grid(True, linestyle="--", color="gray", alpha=0.5)
    ax.set_title(f"The Increasing Concentration of Wealth: {group_desc}", fontsize=16, color="white")
    ax.legend(bbox_to_anchor=(1.05, 1), borderaxespad=0., fontsize=10, handletextpad=2, labelspacing=1.5, borderpad=1)

    # Add sparklines
    for idx, (data, ylabel) in enumerate(zip([N_Bi, totF / N_Bi], [group_label, "Mean Worth"])):
        ax_sparkline = fig.add_axes([0.765, 0.27 - idx * 0.17, 0.23, 0.1])
        ax_sparkline.plot(date, data, color="white", linewidth=1.5, alpha=0.7)
        ax_sparkline.set_xticks([])
        ax_sparkline.grid(color="gray", linestyle="--", axis="y", alpha=0.7)
        ax_sparkline.set_facecolor("none")
        ax_sparkline.set_ylabel(ylabel, fontsize=12, color="white")
        ax_sparkline.spines[:].set_visible(False)

    fig.subplots_adjust(left=0.075, right=0.7, bottom=0.12, top=0.945)
    fig.savefig(f"{group_label}.svg")

    if show:
        plt.show()

def main(min_worth, group_label, group_desc, show):
    """
    Main function to process data and generate plots.

    Args:
    - min_worth: Minimum worth threshold for filtering
    - group_label: Label for the group (e.g., "Billionaires")
    - group_desc: Description of the group
    - show: Whether to display the plot
    """
    files = glob("data/*.csv")
    date = np.array([path[5:].split(".")[0] for path in files], dtype="datetime64[D]")

    N_Bi, totF, totB, totA, totP = np.zeros((5, date.size))

    sorted_indices = np.argsort(date)
    date = date[sorted_indices]

    for i, j in enumerate(sorted_indices):
        totals = process_file_lazy(files[j], min_worth)
        N_Bi[i] = totals["N_Bi"].item()
        totF[i] = totals["totF"].item()
        totB[i] = totals["totB"].item()
        totA[i] = totals["totA"].item()
        totP[i] = totals["totP"].item()

    plot_data(date, totA, totF, totB, totP, N_Bi, group_label, group_desc, show)

if __name__ == "__main__":
    show_plot = len(sys.argv) > 1 and sys.argv[1] == "show"

    main(min_worth=1000, group_label="Billionaires", group_desc=r"Billionaires ($\ge$ 1B)", show=show_plot)
    main(min_worth=1, group_label="Millionaires", group_desc=r"Millionaires ($\ge$ 1M)", show=show_plot)

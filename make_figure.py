import numpy as np
import polars as pl
import matplotlib.pyplot as plt
from glob import glob as gl
import sys

def process_file_lazy(path, min_worth=0):
    """
    Process the file lazily using Polars and return the totals.
    """
    df = pl.scan_csv(
        path,
        schema_overrides={
            "finalWorth": pl.Float64,
            "estWorthPrev": pl.Float64,
            "archivedWorth": pl.Float64,
            "privateAssetsWorth": pl.Float64,
        },
    )
    df_filtered = df.filter(
        (pl.col("finalWorth") >= min_worth)
        & (pl.col("estWorthPrev") >= min_worth)
        & (pl.col("privateAssetsWorth") >= min_worth)
    )
    totals = df_filtered.select(
        [
            pl.len().alias("N_Bi"),
            pl.sum("finalWorth").alias("totF")/1e3,
            pl.sum("estWorthPrev").alias("totB")/1e3,
            pl.sum("archivedWorth").alias("totA")/1e3,
            pl.sum("privateAssetsWorth").alias("totP")/1e3,
        ]
    ).collect()
    return totals[0]

def plot_data(date, totA, totF, totB, totP, N_Bi, group_label, group_desc, show=True):
    """
    Plot the financial data with trendlines and a sparkline directly under the legend.
    """
    plt.rc("text", usetex=True)
    plt.rc("font", family="serif")
    plt.style.use("dark_background")

    fig, ax = plt.subplots(figsize=(10, 6))

    # Main data points
    ax.plot(date, totA, label="Archived Worth", color="slategray", marker=".", linestyle="None")
    ax.plot(date, totF, label="Final Worth", color="lightsteelblue", marker="x", linestyle="None")
    ax.plot(date, totB, label="Begin Worth", color="mediumseagreen", marker="+", linestyle="None")
    ax.plot(date, totP, label="Private Assets", color="darkorchid", marker=".", linestyle="None")

    # Trendlines
    for data, colour, label in zip(
        [totA, totF, totB, totP],
        ["slategray", "lightsteelblue", "mediumseagreen", "darkorchid"],
        ["Archived Worth", "Final Worth", "Begin Worth", "Private Assets"],
    ):
        fit_and_plot_linear_log_trendline(date, data, colour, label)

    ax.set_ylabel("Billions of Dollars", fontsize=12, color="white")
    ax.set_xlabel("Date", fontsize=12, color="white")
    ax.tick_params(axis="x", labelrotation=30, labelsize=10, color="white")
    ax.tick_params(axis="y", labelsize=10, color="white")
    ax.grid(True, linestyle="--", color="gray", alpha=0.5)

    # Add custom title to clarify thresholds
    ax.set_title(f"The Increasing Concentration of Wealth: {group_desc}", fontsize=16, color="white")

    # Legend with proper spacing
    ax.legend(bbox_to_anchor=(1.05, 1), borderaxespad=0., fontsize=10, 
              handletextpad=2, labelspacing=1.5, borderpad=1)

    arst = np.argsort(date)
    # Sparkline at the bottom, placed under the legend
    ax_sparkline = fig.add_axes([0.765, 0.27, 0.23, 0.1])  # Adjusted position under the legend
    ax_sparkline.plot(date[arst], N_Bi[arst], color="white", linewidth=1.5, alpha=0.7)
    ax_sparkline.set_xticks([])
    ax_sparkline.grid(color="gray", linestyle='--', axis="y", alpha=0.7)
    ax_sparkline.set_facecolor("none")  # Transparent background
    ax_sparkline.set_ylabel(f"{group_label}", fontsize=12, color="white")
    ax_sparkline.spines[:].set_visible(False)  # Hide all spines
    
    ax_sparkline = fig.add_axes([0.765, 0.1, 0.23, 0.1])  # Adjusted position under the legend
    ax_sparkline.plot(date[arst], (totF/N_Bi)[arst], color="white", linewidth=1.5, alpha=0.7)
    ax_sparkline.set_xticks([])
    ax_sparkline.grid(color="gray", linestyle='--', axis="y", alpha=0.7)
    ax_sparkline.set_facecolor("none")  # Transparent background
    ax_sparkline.set_ylabel("Mean worth", fontsize=12, color="white")
    ax_sparkline.spines[:].set_visible(False)  # Hide all spines
    # Adjust layout to avoid overlap
    fig.subplots_adjust(left=0.075, right=0.7, bottom=0.12, top=.945)
    fig.savefig(f"{group_label}.svg")
    
    if show:
        plt.show()

def fit_and_plot_linear_log_trendline(date, data, colour, label):
    """
    Perform a linear fit on the log-transformed data and plot the trendline.
    """
    date_numeric = date.astype("float")
    slope, intercept = np.polyfit(date_numeric, np.log(data), 1)
    x_date = np.linspace(date_numeric.min(), date_numeric.max(), 100)
    fitted_data = np.exp(slope * x_date + intercept)
    plt.plot(
        x_date,
        fitted_data,
        color=colour,
        linestyle="--",
        linewidth=1,
        alpha=0.4,
        label=f"{label} Trend:\nDoubling time = {np.log(2)/slope/365.25:0.2f} years",
    )

def main(min_worth=0, group_label="Billionaires", group_desc="Billionaires (≥ 1B)", show=True):
    files = gl("data/*.csv")
    date = np.array([path[5:].split(".")[0] for path in files], dtype="datetime64[D]")

    N_Bi, totF, totB, totA, totP = np.zeros((5, date.size))

    for i, path in enumerate(files):
        totals = process_file_lazy(path, min_worth)
        N_Bi[i], totF[i], totB[i], totA[i], totP[i] = (
            totals["N_Bi"].item(),
            totals["totF"].item(),
            totals["totB"].item(),
            totals["totA"].item(),
            totals["totP"].item(),
        )

    plot_data(date, totA, totF, totB, totP, N_Bi, group_label, group_desc, show)

if __name__ == "__main__":
    # Parse command line arguments
    show_plot = False
    if len(sys.argv) > 1 and sys.argv[1] == "show":
        show_plot = True
    
    # For Billionaires
    main(min_worth=1000, group_label="Billionaires", group_desc="Billionaires ($\\ge$ 1B)", show=show_plot)
    # For Millionaires
    main(min_worth=1, group_label="Millionaires", group_desc="Millionaires ($\\ge$ 1M)", show=show_plot)


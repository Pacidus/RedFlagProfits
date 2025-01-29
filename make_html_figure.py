import sys
import numpy as np
import polars as pl
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from glob import glob
from plotly.subplots import make_subplots

# --------------------------
# Helper Functions
# --------------------------

def convert_latex_for_plotly(text):
    """Convert LaTeX strings to Plotly-compatible format while preserving text elements.
    
    Args:
        text: Input string potentially containing LaTeX math expressions
    
    Returns:
        String formatted for Plotly with text elements wrapped in \text{}
    """
    if '$' not in text:
        return text
    
    parts = text.split('$')
    plotly_str = '$'
    
    # Process alternating text and math components
    for i, part in enumerate(parts):
        if i % 2 == 0:  # Text component
            if part:
                plotly_str += r'\text{' + part + '}'
        else:  # Math component
            plotly_str += ' ' + part + ' '
    
    plotly_str += '$'
    
    # Cleanup empty text elements and extra spaces
    plotly_str = plotly_str.replace(r'\text{}', '').replace('  ', '').replace('$ $', '')
    return plotly_str

# --------------------------
# Data Processing Functions
# --------------------------

def process_file_lazy(file_path, min_worth=0):
    """Process CSV file using Polars lazy API to aggregate financial data.
    
    Args:
        file_path: Path to CSV file containing wealth data
        min_worth: Minimum net worth threshold for inclusion
    
    Returns:
        Dictionary containing:
        - N_Bi: Number of qualifying individuals
        - totF: Total final worth (billions)
        - totB: Total begin worth (billions)
        - totA: Total archived worth (billions)
        - totP: Total private assets (billions)
    """
    result = pl.scan_csv(
        file_path,
        schema_overrides={
            "finalWorth": pl.Float64,
            "estWorthPrev": pl.Float64,
            "archivedWorth": pl.Float64,
            "privateAssetsWorth": pl.Float64,
        },
    ).filter(
        (pl.col("finalWorth") >= min_worth) &
        (pl.col("estWorthPrev") >= min_worth) &
        (pl.col("archivedWorth") >= min_worth) &
        (pl.col("privateAssetsWorth") >= min_worth)
    ).select([
        pl.len().alias("N_Bi"),
        pl.sum("finalWorth").alias("totF") / 1e3,
        pl.sum("estWorthPrev").alias("totB") / 1e3,
        pl.sum("archivedWorth").alias("totA") / 1e3,
        pl.sum("privateAssetsWorth").alias("totP") / 1e3,
    ]).collect()
    
    return result[0]

def calculate_trendline(dates, values):
    """Calculate log-linear trendline and doubling time for wealth data.
    
    Args:
        dates: Array of dates in numeric format
        values: Array of wealth values to model
    
    Returns:
        trend_dates: Numeric dates for trendline plotting
        trend_values: Trendline values
        doubling_time: Estimated doubling time in years
    """
    numeric_dates = dates.astype("float")
    slope, intercept = np.polyfit(numeric_dates, np.log(values), 1)
    trend_dates = np.linspace(numeric_dates.min(), numeric_dates.max(), 100)
    trend_values = np.exp(slope * trend_dates + intercept)
    doubling_time = np.log(2) / slope / 365.25
    return trend_dates, trend_values, doubling_time

# --------------------------
# Plotting Functions
# --------------------------

def plot_wealth_trends(dates, archived, final, begin, private, individual_count, 
                      group_label, group_desc, show=True):
    """Generate dual-format visualizations (Matplotlib SVG + Plotly HTML) for wealth trends.
    
    Args:
        dates: Array of dates for x-axis
        archived: Archived worth data series
        final: Final worth data series
        begin: Begin worth data series
        private: Private assets data series
        individual_count: Number of individuals in group
        group_label: Short identifier for output filenames
        group_desc: Descriptive title for plots
        show: Whether to display Matplotlib plot interactively
    """
    # Configure matplotlib style
    plt.rc("text", usetex=True)
    plt.rc("font", family="serif")
    plt.style.use("dark_background")

    # Create base figure and axes
    fig, main_ax = plt.subplots(figsize=(10, 6))
    colors = ["slategray", "lightsteelblue", "mediumseagreen", "darkorchid"]
    labels = ["Archived Worth", "Final Worth", "Begin Worth", "Private Assets"]
    wealth_data = [archived, final, begin, private]

    # Plot data points and trendlines
    for data, color, label in zip(wealth_data, colors, labels):
        # Raw data points
        main_ax.plot(dates, data, label=label, color=color, marker=".", linestyle="None")
        
        # Trendline calculation and plotting
        trend_dates, trend_values, doubling = calculate_trendline(dates, data)
        main_ax.plot(
            trend_dates,
            trend_values,
            color=color,
            linestyle="--",
            linewidth=1,
            alpha=0.4,
            label=f"{label} Trend:\nDoubling Time = {doubling:.2f} years",
        )

    # Configure main plot appearance
    main_ax.set_ylabel("Billions of Dollars", fontsize=12, color="white")
    main_ax.set_xlabel("Date", fontsize=12, color="white")
    main_ax.tick_params(axis="x", labelrotation=30, labelsize=10, color="white")
    main_ax.tick_params(axis="y", labelsize=10, color="white")
    main_ax.grid(True, linestyle="--", color="gray", alpha=0.5)
    main_ax.set_title(f"The Increasing Concentration of Wealth: {group_desc}", fontsize=16, color="white")
    main_ax.legend(
        bbox_to_anchor=(1.05, 1), 
        borderaxespad=0., 
        fontsize=10, 
        handletextpad=2, 
        labelspacing=1.5, 
        borderpad=1
    )

    # Add right-side sparklines
    for idx, (data, ylabel) in enumerate(zip([individual_count, final / individual_count], 
                                           [group_label, "Mean Worth"])):
        spark_ax = fig.add_axes([0.765, 0.27 - idx * 0.17, 0.23, 0.1])
        spark_ax.plot(dates, data, color="white", linewidth=1.5, alpha=0.7)
        spark_ax.set_xticks([])
        spark_ax.grid(color="gray", linestyle="--", axis="y", alpha=0.7)
        spark_ax.set_facecolor("none")
        spark_ax.set_ylabel(ylabel, fontsize=12, color="white")
        spark_ax.spines[:].set_visible(False)

    # Finalize and save matplotlib figure
    fig.subplots_adjust(left=0.075, right=0.7, bottom=0.12, top=0.945)
    fig.savefig(f"{group_label}.svg")
    if show:
        plt.show()

    # Create Plotly interactive figure
    plotly_fig = make_subplots(
        rows=3,
        cols=2,
        specs=[[{"rowspan": 3}, {}], [None, {}], [None, {}]],
        column_widths=[0.765, 0.235],
        row_heights=[0.65, 0.17, 0.17],
        vertical_spacing=0.08,
        horizontal_spacing=0.08
    )

    # Plotly color scheme and data configuration
    plotly_colors = ["#708090", "#b0c4de", "#3cb371", "#9932cc"]
    labels = ["Archived Worth", "Final Worth", "Begin Worth", "Private Assets"]
    wealth_data = [archived, final, begin, private]

    # Add main traces and trendlines
    for data, color, label in zip(wealth_data, plotly_colors, labels):
        trend_dates, trend_values, doubling = calculate_trendline(dates.astype('datetime64[D]').astype('float'), data)
        plotly_label = convert_latex_for_plotly(label)
        trend_label = convert_latex_for_plotly(f"{label} Trend: {doubling:.1f} yrs")

        # Data points
        plotly_fig.add_trace(go.Scatter(
            x=dates, y=data,
            mode='markers',
            name=plotly_label,
            marker=dict(color=color, size=8),
            hovertemplate="%{x|%Y-%m-%d}<br><b>%{y:.1f}B</b><extra></extra>",
            legendgroup=plotly_label
        ), row=1, col=1)

        # Trendline
        plotly_fig.add_trace(go.Scatter(
            x=trend_dates.astype('datetime64[D]'),
            y=trend_values,
            mode='lines',
            line=dict(color=color, dash='dash', width=2),
            name=trend_label,
            hoverinfo='skip',
            legendgroup=plotly_label,
            showlegend=True
        ), row=1, col=1)

    # Add sparkline traces
    plotly_fig.add_trace(go.Scatter(
        x=dates, y=individual_count,
        mode='lines',
        line=dict(color='white', width=2),
        showlegend=False,
        hoverinfo='skip'
    ), row=2, col=2)

    plotly_fig.add_trace(go.Scatter(
        x=dates, y=final/individual_count,
        mode='lines',
        line=dict(color='white', width=2),
        showlegend=False,
        hoverinfo='skip'
    ), row=3, col=2)

    # Configure plotly layout
    plotly_title = convert_latex_for_plotly(f"The Increasing Concentration of Wealth: {group_desc}")

    plotly_fig.update_layout(
        template='plotly_dark',
        font=dict(family="serif", size=12),
        title=dict(
            text=plotly_title,
            x=0.05,
            font=dict(size=24),
            y=0.95
        ),
        legend=dict(
            x=0.8,
            y=0.95,
            bgcolor='rgba(0,0,0,0)',
            font=dict(size=12),
            xanchor='left'
        ),
        margin=dict(l=80, r=80, t=80, b=80),
        xaxis=dict(
            tickangle=-30,
            tickfont=dict(size=12, family="serif"),
            title=dict(text=convert_latex_for_plotly('Date'), font=dict(size=14))
        ),
        yaxis=dict(
            title=dict(text=convert_latex_for_plotly('Billions of Dollars'), font=dict(size=14)),
            tickfont=dict(size=12, family="serif")
        ),
        hovermode='x unified',
        height=600,
        width=1400,
    )

    # Save plotly figure
    plotly_fig.write_html(f"{group_label}.html", include_plotlyjs='cdn', include_mathjax='cdn')

# --------------------------
# Main Execution
# --------------------------

def main(min_worth, group_label, group_desc, show):
    """Orchestrate data processing and visualization generation.
    
    Args:
        min_worth: Net worth threshold for data filtering
        group_label: Short name for output files
        group_desc: Descriptive title for visualizations
        show: Whether to display Matplotlib plots
    """
    # Locate and process data files
    data_files = glob("data/*.csv")
    dates = np.array([path[5:].split(".")[0] for path in data_files], dtype="datetime64[D]")

    # Initialize data containers
    individual_counts = np.zeros(dates.size)
    final_wealth = np.zeros(dates.size)
    begin_wealth = np.zeros(dates.size)
    archived_wealth = np.zeros(dates.size)
    private_wealth = np.zeros(dates.size)
    
    sorted_indices = np.argsort(dates)
    dates = dates[sorted_indices]

    # Process each file in chronological order
    for i, idx in enumerate(sorted_indices):
        result = process_file_lazy(data_files[idx], min_worth)
        individual_counts[i] = result["N_Bi"].item()
        final_wealth[i] = result["totF"].item()
        begin_wealth[i] = result["totB"].item()
        archived_wealth[i] = result["totA"].item()
        private_wealth[i] = result["totP"].item()

    # Generate visualizations
    plot_wealth_trends(dates, archived_wealth, final_wealth, begin_wealth, private_wealth,
                      individual_counts, group_label, group_desc, show)

if __name__ == "__main__":
    # Generate both billionaire and millionaire visualizations
    display_plot = len(sys.argv) > 1 and sys.argv[1] == "show"
    main(min_worth=1000, group_label="Billionaires", group_desc=r"Billionaires ($\geq$ 1B)", show=display_plot)
    main(min_worth=1, group_label="Millionaires", group_desc=r"Millionaires ($\geq$ 1M)", show=display_plot)

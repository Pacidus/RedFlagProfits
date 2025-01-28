import sys
import numpy as np
import polars as pl
import matplotlib.pyplot as plt
import plotly.graph_objects as go

from glob import glob
from plotly.subplots import make_subplots

def convert_to_plotly_latex(original):
    """Convert LaTeX strings to Plotly-compatible format. Returns original if no LaTeX detected."""
    if '$' not in original:
        return original
    parts = original.split('$')
    plotly_str = '$'
    for i, part in enumerate(parts):
        if i % 2 == 0:
            if part:
                plotly_str += r'\text{' + part + '}'
        else:
            plotly_str += ' ' + part + ' '
    plotly_str += '$'
    # Cleanup empty text and spaces
    plotly_str = plotly_str.replace(r'\text{}', '').replace('  ', ' ').replace('$ $', '$')
    return plotly_str

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


    # Create Plotly figure with adjusted subplot structure
    plotly_fig = make_subplots(
        rows=3,
        cols=2,
        specs=[
            [{"rowspan": 3}, {}],
            [None, {}],
            [None, {}]
        ],  
        column_widths=[.765, .235],  # More space for right column
        row_heights=[.65, .17,.17],      # Adjust row heights
        vertical_spacing=0.08,
        horizontal_spacing=0.08
    )

    colors = ["#708090", "#b0c4de", "#3cb371", "#9932cc"]
    labels = ["Archived Worth", "Final Worth", "Begin Worth", "Private Assets"]
    datasets = [totA, totF, totB, totP]

    for data, color, label in zip(datasets, colors, labels):
        x_date, fitted_data, doubling_time = fit_trendline(date.astype('datetime64[D]').astype('float'), data)
        converted_label = convert_to_plotly_latex(label)
        trend_label = convert_to_plotly_latex(f"{label} Trend: {doubling_time:.1f} yrs")

        # Main plot data points
        plotly_fig.add_trace(go.Scatter(
            x=date, y=data,
            mode='markers',
            name=converted_label,
            marker=dict(color=color, size=8),
            hovertemplate="%{x|%Y-%m-%d}<br><b>%{y:.1f}B</b><extra></extra>",
            legendgroup=converted_label
        ), row=1, col=1)

        # Trendline
        plotly_fig.add_trace(go.Scatter(
            x=x_date.astype('datetime64[D]'),
            y=fitted_data,
            mode='lines',
            line=dict(color=color, dash='dash', width=2),
            name=trend_label,
            hoverinfo='skip',
            legendgroup=converted_label,
            showlegend=True
        ), row=1, col=1)

    # Add sparklines to right column
    # Top right sparkline (N_Bi)
    plotly_fig.add_trace(go.Scatter(
        x=date, y=N_Bi,
        mode='lines',
        line=dict(color='white', width=2),
        showlegend=False,
        hoverinfo='skip'
    ), row=2, col=2)

    # Bottom right sparkline (Mean Worth)
    plotly_fig.add_trace(go.Scatter(
        x=date, y=totF/N_Bi,
        mode='lines',
        line=dict(color='white', width=2),
        showlegend=False,
        hoverinfo='skip'
    ), row=3, col=2)

    # Convert text elements
    converted_group_label = convert_to_plotly_latex(group_label)
    plotly_group_desc = convert_to_plotly_latex(f"The Increasing Concentration of Wealth: {group_desc}")

    # Update sparkline axes
    for row in [2, 3]:
        plotly_fig.update_xaxes(
            showticklabels=False,
            row=row, col=2,
            range=[date.min(), date.max()]
        )
        plotly_fig.update_yaxes(
            showgrid=False,
            tickfont=dict(family="serif", size=10),
            row=row, col=2
        )

    plotly_fig.update_yaxes(
        title_text=converted_group_label,
        row=2, col=2,
        title_font=dict(size=12)
    )
    plotly_fig.update_yaxes(
        title_text=convert_to_plotly_latex("Mean Worth"),
        row=3, col=2,
        title_font=dict(size=12)
    )

    # Layout adjustments
    plotly_fig.update_layout(
        template='plotly_dark',
        font=dict(family="serif", size=12),
        title=dict(
            text=plotly_group_desc,
            x=0.05,
            font=dict(size=24),
            y=0.95
        ),
        legend=dict(
            x=0.8,  # Positioned right of main plot
            y=0.95,  # Aligned with top of sparklines
            bgcolor='rgba(0,0,0,0)',
            font=dict(size=12),
            xanchor='left'
        ),
        margin=dict(l=80, r=80, t=80, b=80),  # Increased right margin
        xaxis=dict(
            tickangle=-30,
            tickfont=dict(size=12, family="serif"),
            title=dict(text=convert_to_plotly_latex('Date'), font=dict(size=14))
        ),
        yaxis=dict(
            title=dict(text=convert_to_plotly_latex('Billions of Dollars'), font=dict(size=14)),
            tickfont=dict(size=12, family="serif")
        ),
        hovermode='x unified',
        height=600,
        width=1400,  # Wider to accommodate elements
    )

    plotly_fig.write_html(f"{group_label}.html", include_plotlyjs='cdn', include_mathjax='cdn')

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
    main(min_worth=1000, group_label="Billionaires", group_desc=r"Billionaires ($\geq$ 1B)", show=show_plot)
    main(min_worth=1, group_label="Millionaires", group_desc=r"Millionaires ($\geq$ 1M)", show=show_plot)

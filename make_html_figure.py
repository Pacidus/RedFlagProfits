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
    """Generate dual-format visualizations (Matplotlib SVG + Plotly HTML) for wealth trends."""
    # =======================================================================
    # Common Parameters for Both Visualizations
    # =======================================================================
    # Data series configuration
    data_labels = ["Archived Worth", "Final Worth", "Begin Worth", "Private Assets"]
    colors = ["slategray", "lightsteelblue", "mediumseagreen", "darkorchid"]
    wealth_data = [archived, final, begin, private]
    
    # Unified styling parameters with library-specific adjustments
    unified_params = {
        # Font styling
        'title_font_size': 16,
        'axis_label_font_size': 12,
        'tick_font_size': 10,
        'legend_font_size': 10,
        'font_family': "serif",
        
        # Layout dimensions
        'base_width': 10,
        'base_height': 6,
        'main_plot_right': 0.7,
        'legend_anchor': (1.05, 1),
        'sparkline_inset': [0.765, 0.29, 0.23, 0.17],
        
        # Grid styling (library-specific values)
        'grid_color': 'rgba(128, 128, 128, 0.5)',
        'matplotlib_grid_style': '--',         # Matplotlib linestyle
        'plotly_grid_dash': 'dash',            # Plotly dash pattern
        'grid_alpha': 0.5,
        
        # Line styles (library-specific values)
        'matplotlib_trend_style': '--',        # Matplotlib dashed line
        'plotly_trend_dash': 'dash',           # Plotly dash pattern
        'trend_linewidth': 1,
        
        # Axis parameters
        'x_tick_rotation': -30,
        'y_axis_label': "Billions of Dollars",
        'sparkline_labels': ["Group Size", "Mean Worth"]
    }

    # =======================================================================
    # Matplotlib SVG Output
    # =======================================================================
    # Initialize figure and axes
    plt.rc("text", usetex=True)
    plt.rc("font", family=unified_params['font_family'])
    plt.style.use("dark_background")
    fig, main_ax = plt.subplots(figsize=(unified_params['base_width'], unified_params['base_height']))

    # Main plot components
    for data, color, label in zip(wealth_data, colors, data_labels):
        main_ax.plot(dates, data, label=label, color=color, marker=".", linestyle="None")
        trend_dates, trend_values, doubling = calculate_trendline(dates, data)
        main_ax.plot(trend_dates, trend_values, color=color, 
                    linestyle=unified_params['matplotlib_trend_style'],
                    linewidth=unified_params['trend_linewidth'], 
                    alpha=0.4,
                    label=f"{label} Trend:\nDoubling Time = {doubling:.2f} years")

    # Axis configuration
    main_ax.set_ylabel(unified_params['y_axis_label'], 
                      fontsize=unified_params['axis_label_font_size'], 
                      color="white")
    main_ax.set_xlabel("Date", 
                      fontsize=unified_params['axis_label_font_size'], 
                      color="white")
    main_ax.tick_params(axis="x", 
                       labelrotation=unified_params['x_tick_rotation'], 
                       labelsize=unified_params['tick_font_size'], 
                       color="white")
    main_ax.tick_params(axis="y", 
                       labelsize=unified_params['tick_font_size'], 
                       color="white")
    main_ax.grid(True, 
                linestyle=unified_params['matplotlib_grid_style'], 
                color="gray", 
                alpha=unified_params['grid_alpha'])
    main_ax.set_title(f"The Increasing Concentration of Wealth: {group_desc}", 
                     fontsize=unified_params['title_font_size'], 
                     color="white")
    main_ax.legend(bbox_to_anchor=unified_params['legend_anchor'], 
                  borderaxespad=0.,
                  fontsize=unified_params['legend_font_size'])

    # Sparkline panels
    for idx, (data, ylabel) in enumerate(zip([individual_count, final/individual_count], 
                                           unified_params['sparkline_labels'])):
        spark_ax = fig.add_axes([
            unified_params['sparkline_inset'][0],
            unified_params['sparkline_inset'][1] - idx * unified_params['sparkline_inset'][3],
            unified_params['sparkline_inset'][2],
            unified_params['sparkline_inset'][3]
        ])
        spark_ax.plot(dates, data, color="white", linewidth=1.5, alpha=0.7)
        spark_ax.set_xticks([])
        spark_ax.grid(color="gray", 
                     linestyle=unified_params['matplotlib_grid_style'], 
                     axis="y", 
                     alpha=0.7)
        spark_ax.set_facecolor("none")
        spark_ax.set_ylabel(ylabel, 
                           fontsize=unified_params['axis_label_font_size'], 
                           color="white")
        spark_ax.spines[:].set_visible(False)

    fig.subplots_adjust(left=0.075, 
                       right=unified_params['main_plot_right'], 
                       bottom=0.12, 
                       top=0.945)
    fig.savefig(f"{group_label}.svg")
    if show: 
        plt.show()

    # =======================================================================
    # Plotly HTML Output
    # =======================================================================
    # Calculate proportional dimensions
    plotly_width = 1000
    plotly_height = int(plotly_width * (unified_params['base_height']/unified_params['base_width'] ))
    plotly_col_widths = [
        1 - unified_params['sparkline_inset'][2],
        unified_params['sparkline_inset'][2]
    ]
    plotly_row_heights = [
        1 - 2 * unified_params['sparkline_inset'][3],
        unified_params['sparkline_inset'][3],
        unified_params['sparkline_inset'][3]
    ]

    plotly_fig = make_subplots(
        rows=3, cols=2,
        specs=[[{"rowspan": 3}, {}], [None, {}], [None, {}]],
        column_widths=plotly_col_widths,
        row_heights=plotly_row_heights,
        vertical_spacing=0.04,
        horizontal_spacing=0.04
    )

    # Main plot traces
    for data, color, label in zip(wealth_data, colors, data_labels):
        trend_dates, trend_values, doubling = calculate_trendline(dates.astype('datetime64[D]').astype('float'), data)
        plotly_label = convert_latex_for_plotly(label)
        trend_label = convert_latex_for_plotly(f"{label} Trend: {doubling:.1f} yrs")

        plotly_fig.add_trace(go.Scatter(
            x=dates, y=data, mode='markers', name=plotly_label,
            marker=dict(color=color, size=8), 
            hovertemplate="%{x|%Y-%m-%d}<br><b>%{y:.1f}B</b>",
            legendgroup=plotly_label
        ), row=1, col=1)

        plotly_fig.add_trace(go.Scatter(
            x=trend_dates.astype('datetime64[D]'), 
            y=trend_values, 
            mode='lines',
            line=dict(color=color, 
                     dash=unified_params['plotly_trend_dash'], 
                     width=unified_params['trend_linewidth']+1),
            name=trend_label,
            hoverinfo='skip', 
            legendgroup=plotly_label
        ), row=1, col=1)

    # Sparkline traces
    plotly_fig.add_trace(go.Scatter(
        x=dates, y=individual_count, mode='lines',
        line=dict(color='white', width=2), 
        showlegend=False, 
        hoverinfo='skip'
    ), row=2, col=2)

    plotly_fig.add_trace(go.Scatter(
        x=dates, y=final/individual_count, mode='lines',
        line=dict(color='white', width=2), 
        showlegend=False, 
        hoverinfo='skip'
    ), row=3, col=2)

    # Unified layout configuration
    plotly_fig.update_layout(
        template='plotly_dark',
        title=dict(
            text=convert_latex_for_plotly(f"The Increasing Concentration of Wealth: {group_desc}"),
            x=0.05,
            font=dict(size=unified_params['title_font_size']),
            y=0.95
        ),
        legend=dict(
            x=unified_params['main_plot_right'] + 0.1,
            y=.95,
            xanchor='left',
            bgcolor='rgba(0,0,0,0)',
            font=dict(size=unified_params['legend_font_size'])
        ),
        margin=dict(
            l=int(plotly_width * 0.075),
            r=int(plotly_width * 0.075),
            t=int(plotly_height * 0.055),
            b=int(plotly_height * 0.12)
        ),
        xaxis=dict(
            tickangle=unified_params['x_tick_rotation'],
            gridcolor=unified_params['grid_color'],
            griddash=unified_params['plotly_grid_dash'],
            title=dict(text=convert_latex_for_plotly('Date'), 
                      font=dict(size=unified_params['axis_label_font_size'])),
            tickfont=dict(size=unified_params['tick_font_size'])
        ),
        yaxis=dict(
            title=dict(text=convert_latex_for_plotly(unified_params['y_axis_label']), 
                      font=dict(size=unified_params['axis_label_font_size'])),
            gridcolor=unified_params['grid_color'],
            griddash=unified_params['plotly_grid_dash'],
            tickfont=dict(size=unified_params['tick_font_size'])
        ),
        hovermode='x unified',
        height=plotly_height,
        width=plotly_width,
        font=dict(
            family=unified_params['font_family'],
            size=unified_params['tick_font_size']
        )
    )

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

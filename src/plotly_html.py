import plotly.graph_objects as go
from plotly.subplots import make_subplots
from common_plot_config import unified_params, data_labels, colors


def convert_latex_for_plotly(text):
    """Convert LaTeX-formatted text to Plotly-compatible format.
    
    Args:
        text: Input string potentially containing LaTeX math mode ($...$)
        
    Returns:
        String formatted for Plotly's LaTeX rendering with proper text wrapping
        
    Example:
        Input: "Average $\\alpha$ value"
        Output: "$\\text{Average }\\alpha\\text{ value}$"
    """
    if "$" not in text:
        return text

    parts = text.split("$")
    plotly_str = "$"
    for i, part in enumerate(parts):
        if i % 2 == 0 and part:  # Text outside math mode
            plotly_str += rf"\text{{{part}}}"
        else:  # Math mode content
            plotly_str += part
    plotly_str += "$"
    
    return plotly_str.replace(r"\text{}", "").replace("$ $", "")


def create_base_figure():
    """Create the initial subplot figure with configured layout dimensions.
    
    Returns:
        plotly.graph_objects.Figure: Configured subplot figure
    """
    width = 1000
    height = int(width * (unified_params["base_height"] / unified_params["base_width"]))
    
    return make_subplots(
        rows=3,
        cols=2,
        specs=[[{"rowspan": 3}, {}], [None, {}], [None, {}]],
        column_widths=[1 - unified_params["sparkline_inset"][2], 
                       unified_params["sparkline_inset"][2]],
        row_heights=[
            1 - 2 * unified_params["sparkline_inset"][3],
            unified_params["sparkline_inset"][3],
            unified_params["sparkline_inset"][3],
        ],
        vertical_spacing=0.04,
        horizontal_spacing=0.04,
    )


def add_main_data_traces(fig, dates, wealth_data, trend_data):
    """Add primary data traces and trend lines to the main plot.
    
    Args:
        fig: Plotly Figure object to add traces to
        dates: List of dates for x-axis
        wealth_data: List of wealth time series arrays
        trend_data: List of trend tuples (dates, values, doubling_time)
    """
    for idx, (data_series, color, label) in enumerate(zip(wealth_data, colors, data_labels)):
        # Unpack trend information
        trend_dates, trend_values, doubling_time = trend_data[idx]
        formatted_label = convert_latex_for_plotly(label)
        trend_label = convert_latex_for_plotly(f"{label} Trend: {doubling_time:.1f} yrs")

        # Add data points
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=data_series,
                mode="markers",
                name=formatted_label,
                marker=dict(color=color, size=8),
                hovertemplate="%{x|%Y-%m-%d}<br><b>%{y:.1f}B</b>",
                legendgroup=formatted_label,
            ),
            row=1,
            col=1
        )

        # Add corresponding trend line
        fig.add_trace(
            go.Scatter(
                x=trend_dates,
                y=trend_values,
                mode="lines",
                line=dict(
                    color=color,
                    dash=unified_params["plotly_trend_dash"],
                    width=unified_params["trend_linewidth"] + 1,
                ),
                name=trend_label,
                hoverinfo="skip",
                legendgroup=formatted_label,
            ),
            row=1,
            col=1
        )


def add_sparklines(fig, dates, individual_count, wealth_data):
    """Add small sparkline graphs to the secondary columns.
    
    Args:
        fig: Plotly Figure object to add traces to
        dates: List of dates for x-axis
        individual_count: Time series for individual counts
        wealth_data: Wealth data used for per-capita calculation
    """
    # Individual count sparkline
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=individual_count,
            mode="lines",
            line=dict(color="white", width=2),
            showlegend=False,
            hoverinfo="skip",
        ),
        row=2,
        col=2
    )

    # Wealth per capita sparkline
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=wealth_data[1] / individual_count,
            mode="lines",
            line=dict(color="white", width=2),
            showlegend=False,
            hoverinfo="skip",
        ),
        row=3,
        col=2
    )

def create_base_figure():
    """Create the initial subplot figure with configured layout dimensions.
    
    Returns tuple: (figure, width, height)
    """
    width = 1000
    height = int(width * (unified_params["base_height"] / unified_params["base_width"]))
    
    fig = make_subplots(
        rows=3,
        cols=2,
        specs=[[{"rowspan": 3}, {}], [None, {}], [None, {}]],
        column_widths=[1 - unified_params["sparkline_inset"][2], 
                       unified_params["sparkline_inset"][2]],
        row_heights=[
            1 - 2 * unified_params["sparkline_inset"][3],
            unified_params["sparkline_inset"][3],
            unified_params["sparkline_inset"][3],
        ],
        vertical_spacing=0.04,
        horizontal_spacing=0.04,
    )
    return fig, width, height

def configure_layout(fig, group_desc, width, height):
    """Apply final layout configuration and styling."""
    margin_config = {
        "l": int(width * 0.075),
        "r": int(width * 0.012),
        "t": int(height * 0.075),
        "b": int(height * 0.12),
    }

    layout_config = {
        "template": "plotly_dark",
        "title": {
            "text": convert_latex_for_plotly(
                f"The Increasing Concentration of Wealth: {group_desc}"
            ),
            "x": 0.05,
            "font": {"size": unified_params["title_font_size"]},
            "y": 0.95,
        },
        "legend": {
            "x": unified_params["main_plot_right"] + 0.05,
            "y": 0.95,
            "xanchor": "left",
            "bgcolor": "rgba(0,0,0,0)",
            "font": {"size": unified_params["legend_font_size"]},
        },
        "margin": margin_config,
        "xaxis": {
            "tickangle": unified_params["x_tick_rotation"],
            "gridcolor": unified_params["grid_color"],
            "griddash": unified_params["plotly_grid_dash"],
            "title": {
                "text": convert_latex_for_plotly("Date"),
                "font": {"size": unified_params["axis_label_font_size"]},
            },
            "tickfont": {"size": unified_params["tick_font_size"]},
        },
        "yaxis": {
            "title": {"text": convert_latex_for_plotly(unified_params["y_axis_label"])},
            "gridcolor": unified_params["grid_color"],
            "griddash": unified_params["plotly_grid_dash"],
            "tickfont": {"size": unified_params["tick_font_size"]},
        },
        "hovermode": "x unified",
        "width": width,
        "height": height,
        "font": {
            "family": unified_params["font_family"],
            "size": unified_params["tick_font_size"],
        },
    }

    fig.update_layout(**layout_config)

def plot_html(dates, wealth_data, individual_count, trend_data, group_label, group_desc):
    """
    Generate and save an interactive HTML visualization of wealth concentration trends.
    """
    fig, width, height = create_base_figure()
    
    add_main_data_traces(fig, dates, wealth_data, trend_data)
    add_sparklines(fig, dates, individual_count, wealth_data)
    configure_layout(fig, group_desc, width, height)

    # Save to HTML file
    fig.write_html(
        f"{group_label}.html",
        include_plotlyjs="cdn",
        include_mathjax="cdn"
    )

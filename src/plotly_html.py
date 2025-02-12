import plotly.graph_objects as go
from plotly.subplots import make_subplots
from common_plot_config import unified_params, data_labels, colors


def convert_latex_for_plotly(text):
    """Convert LaTeX strings to Plotly-compatible format."""
    if "$" not in text:
        return text
    parts = text.split("$")
    plotly_str = "$"
    for i, part in enumerate(parts):
        if i % 2 == 0 and part:
            plotly_str += r"\text{" + part + "}"
        else:
            plotly_str += part
    plotly_str += "$"
    return plotly_str.replace(r"\text{}", "").replace("$ $", "")


def plot_html(
    dates, wealth_data, individual_count, trend_data, group_label, group_desc, show=True
):
    """Generate Plotly HTML visualization."""
    plotly_width = 1000
    plotly_height = int(
        plotly_width * (unified_params["base_height"] / unified_params["base_width"])
    )

    fig = make_subplots(
        rows=3,
        cols=2,
        specs=[[{"rowspan": 3}, {}], [None, {}], [None, {}]],
        column_widths=[
            1 - unified_params["sparkline_inset"][2],
            unified_params["sparkline_inset"][2],
        ],
        row_heights=[
            1 - 2 * unified_params["sparkline_inset"][3],
            unified_params["sparkline_inset"][3],
            unified_params["sparkline_inset"][3],
        ],
        vertical_spacing=0.04,
        horizontal_spacing=0.04,
    )

    # Add main traces
    for i, (data, color, label) in enumerate(zip(wealth_data, colors, data_labels)):
        trend_dates, trend_values, doubling = trend_data[i]
        plotly_label = convert_latex_for_plotly(label)
        trend_label = convert_latex_for_plotly(f"{label} Trend: {doubling:.1f} yrs")

        fig.add_trace(
            go.Scatter(
                x=dates,
                y=data,
                mode="markers",
                name=plotly_label,
                marker=dict(color=color, size=8),
                hovertemplate="%{x|%Y-%m-%d}<br><b>%{y:.1f}B</b>",
                legendgroup=plotly_label,
            ),
            row=1,
            col=1,
        )

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
                legendgroup=plotly_label,
            ),
            row=1,
            col=1,
        )

    # Add sparkline traces
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
        col=2,
    )

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
        col=2,
    )

    # Update layout
    fig.update_layout(
        template="plotly_dark",
        title=dict(
            text=convert_latex_for_plotly(
                f"The Increasing Concentration of Wealth: {group_desc}"
            ),
            x=0.05,
            font=dict(size=unified_params["title_font_size"]),
            y=0.95,
        ),
        legend=dict(
            x=unified_params["main_plot_right"] + 0.1,
            y=0.95,
            xanchor="left",
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=unified_params["legend_font_size"]),
        ),
        margin=dict(
            l=int(plotly_width * 0.075),
            r=int(plotly_width * 0.075),
            t=int(plotly_height * 0.055),
            b=int(plotly_height * 0.12),
        ),
        xaxis=dict(
            tickangle=unified_params["x_tick_rotation"],
            gridcolor=unified_params["grid_color"],
            griddash=unified_params["plotly_grid_dash"],
            title=dict(
                text=convert_latex_for_plotly("Date"),
                font=dict(size=unified_params["axis_label_font_size"]),
            ),
            tickfont=dict(size=unified_params["tick_font_size"]),
        ),
        yaxis=dict(
            title=dict(text=convert_latex_for_plotly(unified_params["y_axis_label"])),
            gridcolor=unified_params["grid_color"],
            griddash=unified_params["plotly_grid_dash"],
            tickfont=dict(size=unified_params["tick_font_size"]),
        ),
        hovermode="x unified",
        height=plotly_height,
        width=plotly_width,
        font=dict(
            family=unified_params["font_family"], size=unified_params["tick_font_size"]
        ),
    )

    fig.write_html(f"{group_label}.html", include_plotlyjs="cdn", include_mathjax="cdn")

import numpy as np
import matplotlib.pyplot as plt
from common_plot_config import unified_params, data_labels, colors


def configure_matplotlib_settings():
    """Configure global matplotlib settings for consistent styling."""
    plt.rc("text", usetex=True)
    plt.rc("font", family=unified_params["font_family"])
    plt.style.use("dark_background")


def create_main_figure():
    """Initialize main figure and axis with unified parameters."""
    fig = plt.figure(
        figsize=(unified_params["base_width"], unified_params["base_height"])
    )
    main_ax = fig.add_subplot(111)
    return fig, main_ax


def plot_wealth_trends(main_ax, dates, wealth_data, trend_data):
    """Plot wealth data points and corresponding trend lines."""
    for i, (data_points, color, label) in enumerate(zip(wealth_data, colors, data_labels)):
        trend_dates, trend_values, doubling_time = trend_data[i]
        
        # Plot raw data points
        main_ax.plot(
            dates, data_points, 
            label=label, 
            color=color, 
            marker=".", 
            linestyle="None"
        )
        
        # Plot trend line
        main_ax.plot(
            trend_dates,
            trend_values,
            color=color,
            linestyle=unified_params["matplotlib_trend_style"],
            linewidth=unified_params["trend_linewidth"],
            alpha=0.4,
            label=f"{label} Trend:\nDoubling Time = {doubling_time:.2f} years",
        )


def configure_main_axes(main_ax, group_desc):
    """Configure main axis labels, ticks, grid, and title."""
    main_ax.set_ylabel(
        unified_params["y_axis_label"],
        fontsize=unified_params["axis_label_font_size"],
        color="white"
    )
    main_ax.set_xlabel(
        "Date", 
        fontsize=unified_params["axis_label_font_size"], 
        color="white"
    )
    
    # Configure ticks
    main_ax.tick_params(
        axis="x",
        labelrotation=unified_params["x_tick_rotation"],
        labelsize=unified_params["tick_font_size"],
        color="white"
    )
    main_ax.tick_params(
        axis="y", 
        labelsize=unified_params["tick_font_size"], 
        color="white"
    )
    
    # Add grid and title
    main_ax.grid(
        True,
        linestyle=unified_params["matplotlib_grid_style"],
        color="gray",
        alpha=unified_params["grid_alpha"]
    )
    main_ax.set_title(
        f"The Increasing Concentration of Wealth: {group_desc}",
        fontsize=unified_params["title_font_size"],
        color="white"
    )


def add_sparklines(fig, dates, individual_count, wealth_data):
    """Add secondary sparkline plots to the main figure."""
    sparkline_data = [
        (individual_count, unified_params["sparkline_labels"][0]),
        (wealth_data[1] / individual_count, unified_params["sparkline_labels"][1])
    ]
    
    for idx, (data, ylabel) in enumerate(sparkline_data):
        # Calculate sparkline position
        spark_pos = [
            unified_params["sparkline_inset"][0],
            unified_params["sparkline_inset"][1] - idx * unified_params["sparkline_inset"][3],
            unified_params["sparkline_inset"][2],
            unified_params["sparkline_inset"][3]
        ]
        
        spark_ax = fig.add_axes(spark_pos)
        spark_ax.plot(dates, data, color="white", linewidth=1.5, alpha=0.7)
        spark_ax.set_xticks([])
        spark_ax.grid(
            color="gray",
            linestyle=unified_params["matplotlib_grid_style"],
            axis="y",
            alpha=0.7
        )
        spark_ax.set_ylabel(
            ylabel, 
            fontsize=unified_params["axis_label_font_size"], 
            color="white"
        )
        spark_ax.spines[:].set_visible(False)


def plot_svg(dates, wealth_data, individual_count, trend_data, group_label, group_desc, show=True):
    """
    Generate wealth concentration visualization with trends and sparklines.
    
    Args:
        dates: Array of datetime objects for x-axis
        wealth_data: List of wealth time series arrays
        individual_count: Array of population count data
        trend_data: List of trend tuples (dates, values, doubling_time)
        group_label: Filename prefix for output
        group_desc: Description for plot title
        show: Whether to display plot interactively
    """
    configure_matplotlib_settings()
    fig, main_ax = create_main_figure()

    # Plot primary data and trends
    plot_wealth_trends(main_ax, dates, wealth_data, trend_data)
    
    # Configure main axes
    configure_main_axes(main_ax, group_desc)
    
    # Add legend
    main_ax.legend(
        bbox_to_anchor=unified_params["legend_anchor"],
        borderaxespad=0.0,
        fontsize=unified_params["legend_font_size"]
    )

    # Add sparkline insets
    add_sparklines(fig, dates, individual_count, wealth_data)

    # Final layout adjustments
    fig.subplots_adjust(
        left=0.075, 
        right=unified_params["main_plot_right"], 
        bottom=0.12, 
        top=0.945
    )
    
    # Save and optionally display
    fig.savefig(f"{group_label}.svg")
    if show:
        plt.show()

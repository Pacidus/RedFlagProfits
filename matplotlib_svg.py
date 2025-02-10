import numpy as np
import matplotlib.pyplot as plt
from common_plot_config import unified_params, data_labels, colors


def plot_svg(
    dates, wealth_data, individual_count, trend_data, group_label, group_desc, show=True
):
    """Generate Matplotlib SVG visualization."""
    plt.rc("text", usetex=True)
    plt.rc("font", family=unified_params["font_family"])
    plt.style.use("dark_background")

    fig, main_ax = plt.subplots(
        figsize=(unified_params["base_width"], unified_params["base_height"])
    )

    # Plot main data and trends
    for i, (data, color, label) in enumerate(zip(wealth_data, colors, data_labels)):
        trend_dates, trend_values, doubling = trend_data[i]
        main_ax.plot(
            dates, data, label=label, color=color, marker=".", linestyle="None"
        )
        main_ax.plot(
            trend_dates,
            trend_values,
            color=color,
            linestyle=unified_params["matplotlib_trend_style"],
            linewidth=unified_params["trend_linewidth"],
            alpha=0.4,
            label=f"{label} Trend:\nDoubling Time = {doubling:.2f} years",
        )

    # Configure axes and legend
    main_ax.set_ylabel(
        unified_params["y_axis_label"],
        fontsize=unified_params["axis_label_font_size"],
        color="white",
    )
    main_ax.set_xlabel(
        "Date", fontsize=unified_params["axis_label_font_size"], color="white"
    )
    main_ax.tick_params(
        axis="x",
        labelrotation=unified_params["x_tick_rotation"],
        labelsize=unified_params["tick_font_size"],
        color="white",
    )
    main_ax.tick_params(
        axis="y", labelsize=unified_params["tick_font_size"], color="white"
    )
    main_ax.grid(
        True,
        linestyle=unified_params["matplotlib_grid_style"],
        color="gray",
        alpha=unified_params["grid_alpha"],
    )
    main_ax.set_title(
        f"The Increasing Concentration of Wealth: {group_desc}",
        fontsize=unified_params["title_font_size"],
        color="white",
    )
    main_ax.legend(
        bbox_to_anchor=unified_params["legend_anchor"],
        borderaxespad=0.0,
        fontsize=unified_params["legend_font_size"],
    )

    # Add sparklines
    for idx, (data, ylabel) in enumerate(
        zip(
            [individual_count, wealth_data[1] / individual_count],
            unified_params["sparkline_labels"],
        )
    ):
        spark_ax = fig.add_axes(
            [
                unified_params["sparkline_inset"][0],
                unified_params["sparkline_inset"][1]
                - idx * unified_params["sparkline_inset"][3],
                unified_params["sparkline_inset"][2],
                unified_params["sparkline_inset"][3],
            ]
        )
        spark_ax.plot(dates, data, color="white", linewidth=1.5, alpha=0.7)
        spark_ax.set_xticks([])
        spark_ax.grid(
            color="gray",
            linestyle=unified_params["matplotlib_grid_style"],
            axis="y",
            alpha=0.7,
        )
        spark_ax.set_ylabel(
            ylabel, fontsize=unified_params["axis_label_font_size"], color="white"
        )
        spark_ax.spines[:].set_visible(False)

    fig.subplots_adjust(
        left=0.075, right=unified_params["main_plot_right"], bottom=0.12, top=0.945
    )
    fig.savefig(f"{group_label}.svg")
    if show:
        plt.show()

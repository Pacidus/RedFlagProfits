# Common configuration and shared variables
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

data_labels = ["Archived Worth", "Final Worth", "Begin Worth", "Private Assets"]
colors = ["slategray", "lightsteelblue", "mediumseagreen", "darkorchid"]

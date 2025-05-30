import numpy as np
import pandas as pd
from urllib.parse import quote


class BackgroundSparklineGenerator:
    COLOR_SCHEMES = {
        "wealth": ("#404040", "#1a1a1a"),
        "count": ("#3a3a3a", "#222222"),
        "average": ("#383838", "#1f1f1f"),
    }

    def __init__(self, card_width=280, card_height=120):
        self.card_width = card_width
        self.card_height = card_height
        self.target_points = 70
        self.padding = 15
        self.window = np.exp(-np.linspace(-2, 2, 40) ** 2)

    def generate_sparkline(self, time_series_data, metric_config):
        """Generate sparkline SVG with unified processing pipeline"""
        clean_data = self._process_data(time_series_data, metric_config)

        coordinates = self._generate_coordinates(
            clean_data, metric_config["curve_style"]
        )
        return self._create_svg(coordinates, metric_config["svg_type"])

    def _process_data(self, data, config):
        """Unified data processing pipeline"""

        wl, length = self.window.shape[0], 10 * self.target_points
        col = config["columns"]
        valid_idx = data[col].dropna().index
        dates = data.loc[valid_idx, "date"]

        # Extract relevant data based on metric type
        if config["type"] == "ratio":
            numerator = data.loc[valid_idx, col[0]] * config.get("scale", 1)
            denominator = data.loc[valid_idx, col[1]].replace(0, 1)
            values = numerator / denominator
        else:  # Single metric
            values = data.loc[valid_idx, col[0]]

        # Prepare time deltas and interpolate
        days = (dates - dates.iloc[0]).dt.days.values
        days = days / days.max()
        fine_grid = np.arange(-wl, length + wl) / length

        # Apply smoothing and downsample
        return np.convolve(self.window, np.interp(fine_grid, days, values))[wl:-wl:10]

    def _generate_coordinates(self, data, curve_style):
        """Generate normalized coordinates with curve application"""
        normalized = (data - data.min()) / np.ptp(data)

        # Map to card coordinates
        x = np.linspace(0, self.card_width, normalized.shape[0] - 1)
        y = (
            self.card_height
            - self.padding
            - normalized * (self.card_height - 2 * self.padding)
        )
        return list(zip(x, y))

    def _create_svg(self, coordinates, svg_type):
        """Generate SVG from coordinates with type-specific coloring"""
        light, dark = self.COLOR_SCHEMES.get(svg_type, ("#404040", "#1a1a1a"))

        # Create polygon points (sparkline + bottom corners)
        points = (
            [(0, self.card_height)]
            + coordinates
            + [(self.card_width, self.card_height)]
        )
        points_str = " ".join(f"{x},{y}" for x, y in points)

        svg_content = f"""
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.card_width} {self.card_height}">
            <rect width="100%" height="100%" fill="{light}"/>
            <polygon points="{points_str}" fill="{dark}" stroke="none"/>
        </svg>
        """
        return f"data:image/svg+xml,{quote(svg_content.strip().replace('\n', ''))}"

    def generate_all_backgrounds(self, dashboard_data):
        """Generate all sparklines with unified configuration"""
        configs = {
            "total_wealth": {
                "type": "single",
                "columns": ["total_wealth"],
                "curve_style": "wealth",
                "svg_type": "wealth",
            },
            "billionaire_count": {
                "type": "single",
                "columns": ["billionaire_count"],
                "curve_style": "count",
                "svg_type": "count",
            },
            "average_wealth": {
                "type": "ratio",
                "columns": ["total_wealth", "billionaire_count"],
                "curve_style": "average",
                "svg_type": "average",
                "scale": 1000,
            },
        }

        time_series = dashboard_data.get("time_series", pd.DataFrame())
        return {
            metric: self.generate_sparkline(time_series, cfg)
            for metric, cfg in configs.items()
        }

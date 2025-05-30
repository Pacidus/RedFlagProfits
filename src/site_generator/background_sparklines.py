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
        self.wind_len = 40
        self.window = np.exp(-np.linspace(-2, 2, self.wind_len) ** 2)

    def gen_sparkline(self, time_series, config):
        """Generate sparkline SVG with unified processing pipeline"""
        clean_data = self._process_data(time_series, config)
        coords = self._generate_coordinates(clean_data)
        return self._create_svg(coords, config["svg_type"])

    def _process_data(self, data, config):
        """Unified data processing pipeline"""
        # Extract and validate data
        cols = config["columns"]
        valid_data = data.dropna(subset=cols)[cols + ["date"]]
        if valid_data.empty:
            return np.zeros(self.target_points)

        # Compute metric values
        if config["type"] == "ratio":
            num = valid_data[cols[0]]
            denom = valid_data[cols[1]].replace(0, 1)
            values = num / denom
        else:
            values = valid_data[cols[0]]

        # Normalize timeline
        days = (valid_data["date"] - valid_data["date"].iloc[0]).dt.days
        days_norm = days / days.max()

        # Create interpolation grid
        interp_length = 10 * self.target_points - (self.wind_len - 1)
        grid = np.arange(-self.wind_len, interp_length + self.wind_len) / interp_length

        # Interpolate and smooth
        interp_vals = np.interp(grid, days_norm, values)
        smoothed = np.convolve(self.window, interp_vals)
        return smoothed[self.wind_len : -self.wind_len : 10]

    def _generate_coordinates(self, data):
        """Generate normalized coordinates"""
        # Safe normalization
        data_min, data_max = data.min(), data.max()
        normalized = (
            (data - data_min) / (data_max - data_min)
            if data_max > data_min
            else np.zeros_like(data)
        )

        # Map to card coordinates
        y = (
            self.card_height
            - self.padding
            - normalized * (self.card_height - 2 * self.padding)
        )
        x = np.linspace(0, self.card_width, len(data))
        return list(zip(x, y))

    def _create_svg(self, coords, svg_type):
        """Generate SVG from coordinates"""
        light, dark = self.COLOR_SCHEMES.get(svg_type, ("#404040", "#1a1a1a"))

        # Create polygon points (sparkline + bottom corners)
        points = (
            [(0, self.card_height)] + coords + [(self.card_width, self.card_height)]
        )
        points_str = " ".join(f"{x},{y}" for x, y in points)

        svg = f"""
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.card_width} {self.card_height}">
            <rect width="100%" height="100%" fill="{light}"/>
            <polygon points="{points_str}" fill="{dark}" stroke="none"/>
        </svg>
        """.strip().replace(
            "\n", ""
        )

        return f"data:image/svg+xml,{quote(svg)}"

    def generate_all_backgrounds(self, dashboard_data):
        """Generate all sparklines with unified configuration"""
        configs = {
            "total_wealth": {
                "type": "single",
                "columns": ["total_wealth"],
                "svg_type": "wealth",
            },
            "billionaire_count": {
                "type": "single",
                "columns": ["billionaire_count"],
                "svg_type": "count",
            },
            "average_wealth": {
                "type": "ratio",
                "columns": ["total_wealth", "billionaire_count"],
                "svg_type": "average",
            },
        }

        ts = dashboard_data.get("time_series", pd.DataFrame())
        return {metric: self.gen_sparkline(ts, cfg) for metric, cfg in configs.items()}

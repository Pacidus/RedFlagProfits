import numpy as np
import pandas as pd
from urllib.parse import quote


class BackgroundSparklineGenerator:
    """Generates clean SVG sparklines for metric card backgrounds."""

    COLOR_SCHEMES = {
        "wealth": {"light": "#404040", "dark": "#1a1a1a"},
        "count": {"light": "#3a3a3a", "dark": "#222222"},
        "average": {"light": "#383838", "dark": "#1f1f1f"},
    }

    CURVE_EXPONENTS = {
        "wealth": 0.85,
        "moderate": 0.95,
        "count": 1.0,  # Linear
        "average": 0.95,  # Moderate
    }

    def __init__(self, card_width=280, card_height=120):
        self.card_width = card_width
        self.card_height = card_height
        self.target_points = 70
        self.padding = 15

    def generate_sparkline(self, time_series_data, metric_config):
        """Unified sparkline generator with configurable metrics."""
        try:
            clean_data = self._get_clean_data(time_series_data, metric_config)
            if len(clean_data) < 2:
                return self._create_fallback_svg()

            coordinates = self._create_coordinates(clean_data, metric_config)
            return self._create_svg(coordinates, metric_config["svg_type"])

        except Exception as e:
            print(f"⚠️  {metric_config['name']} sparkline failed: {e}")
            return self._create_fallback_svg()

    def _get_clean_data(self, data, config):
        """Generic data cleaner with metric-specific processing."""
        if not isinstance(data, pd.DataFrame):
            print("⚠️  Using fallback data")
            return config.get("fallback_data", pd.Series([10, 12, 14, 16]))

        # Process based on metric type
        if config["type"] == "ratio":
            aligned = self._align_columns(data, config["columns"])
            processed = (aligned[0] * config.get("scale", 1)) / aligned[1].replace(0, 1)
        else:
            processed = data[config["columns"][0]].dropna()

        DT = (data["date"] - data["date"].iloc[0]).dt.days
        x_target = np.linspace(0, DT.iloc[-1], (self.target_points + 2) * self.padding)
        vals = np.interp(x_target, DT, processed.values)
        # Apply smoothing and resampling
        window_size = 70
        smoothed = (
            pd.Series(vals, copy=False)
            .rolling(window=window_size, center=True)
            .mean()
            .bfill()
            .ffill()
        )
        return smoothed

    def _align_columns(self, df, columns):
        """Align columns by common indices."""
        common_idx = df.index
        for col in columns:
            common_idx = common_idx.intersection(df[col].dropna().index)
        return [df.loc[common_idx, col] for col in columns]

    def _create_coordinates(self, data, config):
        """Generate coordinates with configurable curve style."""
        # Normalize data
        data_min, data_max = data.min(), data.max()
        data_range = data_max - data_min if data_max != data_min else 1
        normalized = [(x - data_min) / data_range for x in data]

        # Apply curve style
        exponent = self.CURVE_EXPONENTS[config["curve_style"]]
        curved = [x**exponent for x in normalized] if exponent != 1.0 else normalized

        # Map to card coordinates
        x_step = self.card_width / (len(curved) - 1)
        return [
            (
                i * x_step,
                self.card_height
                - (self.padding + y * (self.card_height - 2 * self.padding)),
            )
            for i, y in enumerate(curved)
        ]

    def _create_svg(self, coordinates, svg_type):
        """Generate SVG from coordinates with type-specific coloring."""
        colors = self.COLOR_SCHEMES.get(svg_type, self.COLOR_SCHEMES["wealth"])

        # Create polygon points (sparkline + bottom corners)
        points = (
            [(0, self.card_height)]
            + coordinates
            + [(self.card_width, self.card_height)]
        )
        points_str = " ".join(f"{x},{y}" for x, y in points)

        svg_content = f"""
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.card_width} {self.card_height}">
            <rect width="100%" height="100%" fill="{colors['light']}"/>
            <polygon points="{points_str}" fill="{colors['dark']}" stroke="none"/>
        </svg>
        """
        return f"data:image/svg+xml,{quote(svg_content.strip().replace('\n', ''))}"

    def _create_fallback_svg(self):
        """Simple fallback SVG showing generic growth curve."""
        return self._create_svg(
            [
                (i * self.card_width / 6, self.card_height * (0.8 - 0.6 * (i / 6) ** 2))
                for i in range(7)
            ],
            "wealth",
        )

    def generate_all_backgrounds(self, dashboard_data):
        """Generate all sparklines with unified configuration."""
        if dashboard_data.get("time_series").empty:
            print("⚠️  No time series data available")
            fallback = self._create_fallback_svg()
            return {
                k: fallback
                for k in ["total_wealth", "billionaire_count", "average_wealth"]
            }

        configs = {
            "total_wealth": {
                "name": "Total wealth",
                "type": "single",
                "columns": ["total_wealth"],
                "curve_style": "wealth",
                "svg_type": "wealth",
                "fallback_data": pd.Series([10, 12, 14, 16, 16.6]),
            },
            "billionaire_count": {
                "name": "Billionaire count",
                "type": "single",
                "columns": ["billionaire_count"],
                "curve_style": "count",
                "svg_type": "count",
                "fallback_data": pd.Series([2500, 2600, 2700, 2800, 2900]),
            },
            "average_wealth": {
                "name": "Average wealth",
                "type": "ratio",
                "columns": ["total_wealth", "billionaire_count"],
                "curve_style": "average",
                "svg_type": "average",
                "scale": 1000,  # Convert trillions to billions
                "fallback_data": pd.Series([4.0, 4.2, 4.5, 4.8, 5.1]),
            },
        }

        return {
            metric: self.generate_sparkline(dashboard_data["time_series"], cfg)
            for metric, cfg in configs.items()
        }

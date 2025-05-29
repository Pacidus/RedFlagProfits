"""Chart data preparation for Red Flags Profits.

Save this file as: src/site_generator/chart_data_processor.py
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json


class ChartDataProcessor:
    """Processes raw data into chart-ready formats."""

    def __init__(self):
        # No logger needed - use print() to match RedFlagsSiteGenerator pattern
        pass

    def prepare_wealth_timeline_data(
        self, time_series_df, current_cpi=None, base_cpi=None
    ):
        """Prepare wealth timeline data - ONLY total accumulated wealth."""

        if time_series_df is None or len(time_series_df) < 2:
            print("⚠️  Insufficient data for wealth timeline chart")
            return self._get_fallback_chart_data()

        # Ensure we have clean data
        df = time_series_df.copy().sort_values("date")
        df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")

        # Only nominal wealth (raw data in trillions)
        chart_data = {
            "data": [
                {"x": row["date_str"], "y": row["total_wealth"]}
                for _, row in df.iterrows()
            ],
            "timeRange": {
                "start": df.iloc[0]["date_str"],
                "end": df.iloc[-1]["date_str"],
                "totalDays": (df.iloc[-1]["date"] - df.iloc[0]["date"]).days,
            },
            "title": "Total Billionaire Wealth",
            "yAxisTitle": "Wealth (Trillions USD)",
        }

        # Summary statistics
        latest = df.iloc[-1]
        first = df.iloc[0]

        chart_data["summary"] = {
            "totalIncrease": (
                (latest["total_wealth"] - first["total_wealth"])
                / first["total_wealth"]
                * 100
            ),
            "timespan": f"{first['date'].strftime('%Y-%m-%d')} to {latest['date'].strftime('%Y-%m-%d')}",
            "dataPoints": len(df),
            "startValue": first["total_wealth"],
            "endValue": latest["total_wealth"],
        }

        print(f"✅ Prepared wealth timeline chart with {len(df)} data points")
        return chart_data

    def _get_fallback_chart_data(self):
        """Return fallback data when insufficient real data is available."""
        # Generate realistic-looking sample data
        dates = pd.date_range(start="2024-01-01", end="2024-12-31", freq="W")
        wealth_values = []

        base_wealth = 10.0  # Starting at 10 trillion
        for i, date in enumerate(dates):
            # Simulate exponential growth with some noise
            growth = base_wealth * (1.08 ** (i / 52))  # 8% annual growth
            noise = np.random.normal(0, 0.1)  # Small random variation
            wealth_values.append(max(growth + noise, base_wealth))

        return {
            "data": [
                {"x": date.strftime("%Y-%m-%d"), "y": wealth}
                for date, wealth in zip(dates, wealth_values)
            ],
            "timeRange": {
                "start": dates[0].strftime("%Y-%m-%d"),
                "end": dates[-1].strftime("%Y-%m-%d"),
                "totalDays": 365,
            },
            "title": "Total Billionaire Wealth (Sample)",
            "yAxisTitle": "Wealth (Trillions USD)",
            "summary": {
                "totalIncrease": 25.0,
                "timespan": "Sample data for demonstration",
                "dataPoints": len(dates),
                "startValue": base_wealth,
                "endValue": wealth_values[-1],
            },
        }

    def export_chart_data_to_json(self, chart_config, output_path):
        """Export chart configuration to JSON file for JavaScript consumption."""
        try:
            with open(output_path, "w") as f:
                json.dump(chart_config, f, indent=2, default=str)
            print(f"💾 Chart data exported to {output_path}")
            return True
        except Exception as e:
            print(f"❌ Failed to export chart data: {e}")
            return False

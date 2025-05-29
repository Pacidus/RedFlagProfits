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
        pass

    def prepare_wealth_timeline_data(
        self, time_series_df, current_cpi=None, base_cpi=None
    ):
        """Prepare wealth timeline data with exponential fit."""

        if time_series_df is None or len(time_series_df) < 2:
            print("⚠️  Insufficient data for wealth timeline chart")
            return self._get_fallback_chart_data()

        # Ensure we have clean data
        df = time_series_df.copy().sort_values("date")
        df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")

        # Calculate days from start for fitting
        df["days_from_start"] = (df["date"] - df["date"].iloc[0]).dt.days

        # Prepare nominal data
        nominal_data = [
            {"x": row["date_str"], "y": row["total_wealth"]} for _, row in df.iterrows()
        ]

        # Calculate exponential fit using log-log regression
        fit_params = self._calculate_exponential_fit(df)

        # Generate trend line data
        trend_data = self._generate_trend_line(df, fit_params)

        # Prepare inflation-adjusted data if CPI available
        inflation_data = None
        if "cpi_u" in df.columns and not df["cpi_u"].isna().all():
            inflation_data = self._prepare_inflation_adjusted_data(df, "cpi_u")
        elif "pce" in df.columns and not df["pce"].isna().all():
            inflation_data = self._prepare_inflation_adjusted_data(df, "pce")

        chart_data = {
            "data": nominal_data,
            "trendLine": trend_data,
            "fitParams": fit_params,
            "inflationData": inflation_data,
            "timeRange": {
                "start": df.iloc[0]["date_str"],
                "end": df.iloc[-1]["date_str"],
                "totalDays": (df.iloc[-1]["date"] - df.iloc[0]["date"]).days,
            },
            "title": "Total Billionaire Wealth",
            "yAxisTitle": "Wealth (Trillions USD)",
            "animation": {
                "pointDelay": 10,  # ms between points
                "trendLineSpeed": 1500,  # ms for trend line animation
            },
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
            "exponentialGrowthRate": fit_params["annualGrowthRate"],
        }

        print(
            f"✅ Prepared wealth timeline with exponential fit (R² = {fit_params['r_squared']:.3f})"
        )
        return chart_data

    def _calculate_exponential_fit(self, df):
        """Calculate exponential fit parameters using log-log regression."""
        # Remove any zero or negative values
        valid_data = df[df["total_wealth"] > 0].copy()

        if len(valid_data) < 2:
            return {"a": 1, "b": 0, "r_squared": 0, "annualGrowthRate": 0}

        # Log transform for linear regression
        x = valid_data["days_from_start"].values
        y = np.log(valid_data["total_wealth"].values)

        # Perform linear regression on log data
        coeffs = np.polyfit(x, y, 1)
        b = coeffs[0]  # Growth rate (per day)
        log_a = coeffs[1]  # Log of initial value
        a = np.exp(log_a)

        # Calculate R-squared
        y_pred = b * x + log_a
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

        # Convert daily growth rate to annual
        annual_growth_rate = (np.exp(b * 365.25) - 1) * 100

        return {
            "a": a,
            "b": b,
            "r_squared": r_squared,
            "annualGrowthRate": annual_growth_rate,
        }

    def _generate_trend_line(self, df, fit_params):
        """Generate smooth trend line data."""
        # Create 100 evenly spaced points for smooth curve
        days_range = df["days_from_start"].max()
        trend_days = np.linspace(0, days_range, 100)

        # Calculate trend values: y = a * e^(b*x)
        trend_values = fit_params["a"] * np.exp(fit_params["b"] * trend_days)

        # Convert back to dates
        start_date = df["date"].iloc[0]
        trend_data = []

        for i, days in enumerate(trend_days):
            date = start_date + pd.Timedelta(days=int(days))
            trend_data.append(
                {"x": date.strftime("%Y-%m-%d"), "y": float(trend_values[i])}
            )

        return trend_data

    def _prepare_inflation_adjusted_data(self, df, inflation_column):
        """Prepare inflation-adjusted data."""
        # Get base inflation value (first non-null value)
        base_inflation = (
            df[inflation_column].dropna().iloc[0]
            if not df[inflation_column].isna().all()
            else None
        )

        if base_inflation is None:
            return None

        adjusted_data = []
        for _, row in df.iterrows():
            if pd.notna(row[inflation_column]) and row[inflation_column] > 0:
                # Adjust for inflation relative to base period
                adjustment_factor = base_inflation / row[inflation_column]
                adjusted_value = row["total_wealth"] * adjustment_factor
                adjusted_data.append({"x": row["date_str"], "y": adjusted_value})

        return {
            "data": adjusted_data,
            "inflationType": inflation_column.upper(),
            "baseValue": base_inflation,
        }

    def _get_fallback_chart_data(self):
        """Return fallback data when insufficient real data is available."""
        dates = pd.date_range(start="2024-01-01", end="2024-12-31", freq="W")
        wealth_values = []

        base_wealth = 10.0
        for i, date in enumerate(dates):
            growth = base_wealth * (1.08 ** (i / 52))
            noise = np.random.normal(0, 0.1)
            wealth_values.append(max(growth + noise, base_wealth))

        return {
            "data": [
                {"x": date.strftime("%Y-%m-%d"), "y": wealth}
                for date, wealth in zip(dates, wealth_values)
            ],
            "trendLine": [],
            "fitParams": {
                "a": 10,
                "b": 0.0002,
                "r_squared": 0.95,
                "annualGrowthRate": 8,
            },
            "inflationData": None,
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
                "exponentialGrowthRate": 8.0,
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

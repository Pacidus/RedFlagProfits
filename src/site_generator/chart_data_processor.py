"""Chart data preparation for Red Flags Profits."""

import pandas as pd
import numpy as np
import json


class ChartDataProcessor:
    """Processes raw data into chart-ready formats."""

    def prepare_wealth_timeline_data(self, time_series_df):
        """Prepare wealth timeline data with exponential fit."""

        df = time_series_df.copy().sort_values("date")
        df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")
        df["days_from_start"] = (df["date"] - df.iloc[0]["date"]).dt.days

        nominal_data = [
            {"x": row["date_str"], "y": row["total_wealth"]} for _, row in df.iterrows()
        ]

        fit_params = self._calculate_exponential_fit(df)
        trend_data = self._generate_trend_line(df, fit_params)
        inflation_data = self._get_inflation_data(df)

        chart_data = {
            "data": nominal_data,
            "trendLine": trend_data,
            "fitParams": fit_params,
            "inflationData": inflation_data,
            "timeRange": self._get_time_range(df),
            "title": "Total Billionaire Wealth",
            "yAxisTitle": "Wealth (Trillions USD)",
            "animation": {"pointDelay": 10, "trendLineSpeed": 1500},
            "summary": self._get_summary_stats(df, fit_params),
        }

        print(f"✅ Prepared wealth timeline (R² = {fit_params['r_squared']:.3f})")
        return chart_data

    def _calculate_exponential_fit(self, df):
        """Calculate exponential fit parameters using log-log regression."""
        valid_data = df[df["total_wealth"] > 0]
        valid_data = valid_data[valid_data["date"] > pd.Timestamp(2022, 10, 14)]
        if len(valid_data) < 2:
            return {"a": 1, "b": 0, "r_squared": 0, "annualGrowthRate": 0}

        x = valid_data["days_from_start"].values
        y = np.log(valid_data["total_wealth"].values)
        b, log_a = np.polyfit(x, y, 1)
        a = np.exp(log_a)

        y_pred = b * x + log_a
        ss_res, ss_tot = np.sum([(y - y_pred) ** 2, (y - np.mean(y)) ** 2], 1)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
        annual_growth_rate = (np.exp(b * 365.25) - 1) * 100

        return {
            "a": a,
            "b": b,
            "r_squared": r_squared,
            "annualGrowthRate": annual_growth_rate,
        }

    def _generate_trend_line(self, df, fit_params):
        """Generate smooth trend line data."""
        days_range = df["days_from_start"].max()
        trend_days = np.linspace(0, days_range, 100)
        start_date = df["date"].iloc[0]

        return [
            {
                "x": (start_date + pd.Timedelta(days=int(days))).strftime("%Y-%m-%d"),
                "y": float(fit_params["a"] * np.exp(fit_params["b"] * days)),
            }
            for days in trend_days
        ]

    def _get_inflation_data(self, df):
        """Get inflation-adjusted data if available."""
        for col in ["cpi_u", "pce"]:
            if col in df.columns and not df[col].isna().all():
                return self._prepare_inflation_adjusted_data(df, col)
        return None

    def _prepare_inflation_adjusted_data(self, df, inflation_column):
        """Prepare inflation-adjusted data."""
        base_inflation = df[inflation_column].dropna().iloc[0]
        return {
            "data": [
                {
                    "x": row["date_str"],
                    "y": row["total_wealth"] * (base_inflation / row[inflation_column]),
                }
                for _, row in df.iterrows()
                if pd.notna(row[inflation_column]) and row[inflation_column] > 0
            ],
            "inflationType": inflation_column.upper(),
            "baseValue": base_inflation,
        }

    def _get_time_range(self, df):
        """Get time range metadata."""
        return {
            "start": df.iloc[0]["date_str"],
            "end": df.iloc[-1]["date_str"],
            "totalDays": (df["date"].iloc[-1] - df["date"].iloc[0]).days,
        }

    def _get_summary_stats(self, df, fit_params):
        """Calculate summary statistics."""
        latest, first = df.iloc[-1], df.iloc[0]
        return {
            "totalIncrease": (
                (latest["total_wealth"] - first["total_wealth"]) / first["total_wealth"]
            )
            * 100,
            "timespan": f"{first['date'].strftime('%Y-%m-%d')} to {latest['date'].strftime('%Y-%m-%d')}",
            "dataPoints": len(df),
            "startValue": first["total_wealth"],
            "endValue": latest["total_wealth"],
            "exponentialGrowthRate": fit_params["annualGrowthRate"],
        }

    def export_chart_data_to_json(self, chart_config, output_path):
        """Export chart configuration to JSON file."""
        try:
            with open(output_path, "w") as f:
                json.dump(chart_config, f, indent=2, default=str)
            print(f"💾 Chart data exported to {output_path}")
            return True
        except Exception as e:
            print(f"❌ Failed to export chart data: {e}")
            return False

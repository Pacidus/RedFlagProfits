"""Chart data preparation for Red Flags Profits with enhanced debugging."""

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

        # Calculate nominal fit parameters
        fit_params = self._calculate_exponential_fit(df)
        trend_data = self._generate_trend_line(df, fit_params)

        # Get inflation data and calculate inflation-adjusted metrics
        inflation_data = self._get_inflation_data(df)
        inflation_fit_params = None
        inflation_trend_data = None
        inflation_df = None

        if inflation_data:
            # Create a temporary dataframe with inflation-adjusted values for fit calculation
            inflation_df = pd.DataFrame(inflation_data["data"])
            inflation_df["date"] = pd.to_datetime(inflation_df["x"])
            inflation_df["total_wealth"] = inflation_df["y"]
            inflation_df["days_from_start"] = (
                inflation_df["date"] - inflation_df["date"].iloc[0]
            ).dt.days

            # Calculate exponential fit for inflation-adjusted data
            inflation_fit_params = self._calculate_exponential_fit(inflation_df)
            inflation_trend_data = self._generate_trend_line(
                inflation_df, inflation_fit_params
            )
            print(
                f"📊 Inflation-adjusted growth rate: {inflation_fit_params['annualGrowthRate']:.1f}% vs nominal: {fit_params['annualGrowthRate']:.1f}%"
            )

        chart_data = {
            "data": nominal_data,
            "trendLine": trend_data,
            "inflationTrendLine": inflation_trend_data,  # Add inflation-adjusted trend line
            "fitParams": fit_params,
            "inflationFitParams": inflation_fit_params,  # Add inflation-adjusted fit parameters
            "inflationData": inflation_data,
            "timeRange": self._get_time_range(df),
            "title": "Total Billionaire Wealth",
            "yAxisTitle": "Wealth (Trillions USD)",
            "animation": {"pointDelay": 5, "trendLineSpeed": 800},  # Faster animation
            "summary": self._get_summary_stats(df, fit_params),
            "inflationSummary": (
                self._get_inflation_summary_stats(inflation_df, inflation_fit_params)
                if inflation_data
                else None
            ),
        }

        print(f"✅ Prepared wealth timeline (R² = {fit_params['r_squared']:.3f})")

        # Enhanced debugging for inflation data
        if inflation_data:
            print(
                f"✅ Inflation data available: {inflation_data['inflationType']} with {len(inflation_data['data'])} points"
            )
            if inflation_fit_params:
                print(
                    f"✅ Inflation-adjusted exponential fit calculated (R² = {inflation_fit_params['r_squared']:.3f})"
                )
        else:
            print("⚠️  No inflation data generated - checking reasons:")
            self._debug_inflation_data(df)

        return chart_data

    def _debug_inflation_data(self, df):
        """Debug why inflation data is not available."""
        inflation_cols = ["cpi_u", "pce"]

        for col in inflation_cols:
            if col not in df.columns:
                print(f"   ❌ Column '{col}' missing from dataset")
            else:
                non_null_count = df[col].notna().sum()
                total_count = len(df)
                print(f"   📊 {col}: {non_null_count}/{total_count} non-null values")

                if non_null_count == 0:
                    print(
                        f"      ⚠️  All {col} values are null - FRED API might have failed"
                    )
                elif non_null_count < total_count / 2:
                    print(f"      ⚠️  {col} has many null values - partial FRED data")
                else:
                    print(f"      ✅ {col} looks good")

                    # Show sample values
                    sample_values = df[df[col].notna()][col].head(3).tolist()
                    print(f"      Sample values: {sample_values}")

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
        """Get inflation-adjusted data if available with better debugging."""
        print(f"🔍 Checking inflation data in time series...")
        print(f"   Available columns: {list(df.columns)}")
        print(f"   Data shape: {df.shape}")

        for col in ["cpi_u", "pce"]:
            if col in df.columns:
                non_null_count = df[col].notna().sum()
                total_count = len(df)
                print(
                    f"   {col.upper()}: {non_null_count}/{total_count} non-null values"
                )

                if non_null_count > 0:
                    print(f"🔄 Using {col.upper()} for inflation adjustment")
                    return self._prepare_inflation_adjusted_data(df, col)
            else:
                print(f"   ❌ {col.upper()} column not found")

        print("❌ No usable inflation data found in time series")
        return None

    def _prepare_inflation_adjusted_data(self, df, inflation_column):
        """Prepare inflation-adjusted data normalized to TODAY'S dollar value."""
        # Get base inflation value (LAST/most recent non-null value for today's dollars)
        non_null_data = df[inflation_column].dropna()
        if non_null_data.size == 0:
            print(f"❌ No valid inflation values found for {inflation_column}")
            return None

        base_inflation = non_null_data.iloc[-1]  # Use LATEST value (today's dollars)
        base_date = df[df[inflation_column].notna()].iloc[-1]["date_str"]

        print(
            f"📊 Base {inflation_column.upper()} value: {base_inflation:.2f} (from {base_date})"
        )
        print(
            f"💰 Adjusting all historical values to {base_date} dollar purchasing power"
        )

        inflation_adjusted_data = []
        for _, row in df.iterrows():
            if pd.notna(row[inflation_column]) and row[inflation_column] > 0:
                # Adjust historical values UP to today's dollar value
                adjusted_value = row["total_wealth"] * (
                    base_inflation / row[inflation_column]
                )
                inflation_adjusted_data.append(
                    {
                        "x": row["date_str"],
                        "y": adjusted_value,
                    }
                )

        print(
            f"📊 Generated {len(inflation_adjusted_data)} inflation-adjusted points (normalized to latest dollars)"
        )

        return {
            "data": inflation_adjusted_data,
            "inflationType": inflation_column.upper().replace("_", "-"),
            "baseValue": base_inflation,
            "baseDate": base_date,
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

    def _get_inflation_summary_stats(self, inflation_df, inflation_fit_params):
        """Calculate summary statistics for inflation-adjusted data."""
        if inflation_df is None or inflation_fit_params is None:
            print("⚠️  Cannot calculate inflation summary: missing data or fit params")
            return None

        latest, first = inflation_df.iloc[-1], inflation_df.iloc[0]

        total_increase = (
            (latest["total_wealth"] - first["total_wealth"]) / first["total_wealth"]
        ) * 100

        summary = {
            "totalIncrease": total_increase,
            "timespan": f"{first['date'].strftime('%Y-%m-%d')} to {latest['date'].strftime('%Y-%m-%d')}",
            "dataPoints": len(inflation_df),
            "startValue": first["total_wealth"],
            "endValue": latest["total_wealth"],
            "exponentialGrowthRate": inflation_fit_params["annualGrowthRate"],
        }

        print(f"📊 Inflation summary calculated:")
        print(
            f"   Start: ${summary['startValue']:.1f}T → End: ${summary['endValue']:.1f}T"
        )
        print(f"   Total increase: {summary['totalIncrease']:.1f}%")
        print(f"   Growth rate: {summary['exponentialGrowthRate']:.1f}% per year")

        return summary

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

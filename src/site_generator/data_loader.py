"""Data loading utilities for site generation."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path


class DataLoader:
    """Loads and processes data for site generation."""

    def __init__(self, data_file="data/all_billionaires.parquet"):
        self.data_file = Path(data_file)

    def load_latest_data(self):
        """Load the most recent data."""
        if not self.data_file.exists():
            raise FileNotFoundError(f"Data file not found: {self.data_file}")

        df = pd.read_parquet(self.data_file)
        df["crawl_date"] = pd.to_datetime(df["crawl_date"])
        return df.sort_values("crawl_date")

    def calculate_metrics(self, df):
        """Calculate key metrics from the data - COMPUTED FRESH EACH TIME."""
        print("📊 Computing dashboard metrics from data...")

        # Group by date for time series analysis
        daily_totals = (
            df.groupby("crawl_date")
            .agg(
                {
                    "finalWorth": "sum",
                    "personName": "nunique",  # Count unique billionaires per day
                }
            )
            .reset_index()
        )

        daily_totals.columns = ["date", "total_wealth", "billionaire_count"]
        daily_totals = daily_totals.sort_values("date")

        if len(daily_totals) == 0:
            raise ValueError("No data available for calculations")

        # Get actual date range from the dataset (actual timespan, not data point count)
        data_start_date = daily_totals.iloc[0]["date"]
        data_end_date = daily_totals.iloc[-1]["date"]
        data_days_span = (data_end_date - data_start_date).days  # Actual timespan
        data_points = len(daily_totals)  # Number of data collection points

        print(
            f"📅 Dataset timespan: {data_start_date.strftime('%Y-%m-%d')} to {data_end_date.strftime('%Y-%m-%d')} ({data_days_span} days)"
        )
        print(f"📊 Data collection points: {data_points} days with data")

        # Current metrics (latest data point)
        latest = daily_totals.iloc[-1]

        # FIXED: Keep everything in consistent units - store in trillions from the start
        current_total_wealth_trillions = (
            latest["total_wealth"] / 1000000
        )  # Convert from millions to trillions
        current_billionaire_count = int(latest["billionaire_count"])
        current_avg_wealth_billions = (
            current_total_wealth_trillions
            * 1000
            / current_billionaire_count  # Trillions * 1000 = billions
            if current_billionaire_count > 0
            else 0
        )

        # Historical comparison metrics (first vs latest for stability)
        first = daily_totals.iloc[0]
        first_total_wealth_trillions = (
            first["total_wealth"] / 1000000
        )  # Convert from millions to trillions
        first_billionaire_count = int(first["billionaire_count"])

        # Calculate growth rates using STABLE metrics (first to latest)
        # Convert daily_totals to trillions for consistency
        daily_totals_trillions = daily_totals.copy()
        daily_totals_trillions["total_wealth"] = (
            daily_totals_trillions["total_wealth"] / 1000000
        )

        growth_metrics = self._calculate_growth_metrics(daily_totals_trillions)

        # Calculate increases from first data point (retrieved from dataset)
        wealth_increase = (
            (
                (current_total_wealth_trillions - first_total_wealth_trillions)
                / first_total_wealth_trillions
                * 100
            )
            if first_total_wealth_trillions > 0
            else 0
        )
        billionaire_increase = current_billionaire_count - first_billionaire_count

        # Calculate average wealth change
        first_avg_billions = (
            first_total_wealth_trillions
            * 1000
            / first_billionaire_count  # Trillions * 1000 = billions
            if first_billionaire_count > 0
            else 0
        )
        avg_wealth_increase = (
            (
                (current_avg_wealth_billions - first_avg_billions)
                / first_avg_billions
                * 100
            )
            if first_avg_billions > 0
            else 0
        )

        print(
            f"✅ Metrics computed: {current_billionaire_count:,} billionaires, ${current_total_wealth_trillions:.1f}T total"
        )
        print(
            f"📈 Growth: {growth_metrics['annual_growth_rate']:.1f}% CAGR, {wealth_increase:.1f}% total increase"
        )

        return {
            # FIXED: Store values in their final display units
            "total_wealth_trillions": current_total_wealth_trillions,  # In trillions (16.6)
            "billionaire_count": current_billionaire_count,
            "average_wealth_billions": current_avg_wealth_billions,  # In billions (5.5)
            "growth_rate": growth_metrics["annual_growth_rate"],
            "doubling_time": growth_metrics["doubling_time"],
            "daily_accumulation": growth_metrics["daily_accumulation"],
            "last_updated": data_end_date,
            # Historical increases (from first data point retrieved from dataset)
            "wealth_increase_pct": wealth_increase,
            "billionaire_increase_count": billionaire_increase,
            "avg_wealth_increase_pct": avg_wealth_increase,
            # Data range info (retrieved from actual dataset)
            "data_start_date": data_start_date,
            "data_end_date": data_end_date,
            "data_days_span": data_days_span,  # Actual timespan in days
            "data_points": data_points,  # Number of collection points
            # Time series data for potential charts (also in trillions)
            "time_series": daily_totals_trillions,
        }

    def _calculate_growth_metrics(self, daily_totals):
        """Calculate growth metrics using STABLE time series approach."""
        if len(daily_totals) < 2:
            return {
                "annual_growth_rate": 0.0,
                "doubling_time": float("inf"),
                "daily_accumulation": 0.0,
            }

        # Use monthly averages for more stable growth calculation
        monthly_data = self._get_monthly_averages(daily_totals)

        if len(monthly_data) < 2:
            # Fallback to daily data if insufficient monthly data
            start_value = daily_totals.iloc[0]["total_wealth"]
            end_value = daily_totals.iloc[-1]["total_wealth"]
            start_date = daily_totals.iloc[0]["date"]
            end_date = daily_totals.iloc[-1]["date"]
            print("⚠️  Using daily data for CAGR (insufficient monthly data)")
        else:
            # Use monthly averages for stability
            start_value = monthly_data.iloc[0]["total_wealth"]
            end_value = monthly_data.iloc[-1]["total_wealth"]
            start_date = monthly_data.iloc[0]["date"]
            end_date = monthly_data.iloc[-1]["date"]
            print(
                f"✅ Using monthly averages for stable CAGR calculation ({len(monthly_data)} months)"
            )

        # Calculate actual time span (not just data points)
        days_diff = (end_date - start_date).days

        if days_diff == 0 or start_value == 0:
            return {
                "annual_growth_rate": 0.0,
                "doubling_time": float("inf"),
                "daily_accumulation": 0.0,
            }

        # Calculate CAGR (Compound Annual Growth Rate)
        years_diff = days_diff / 365.25
        annual_growth_rate = ((end_value / start_value) ** (1 / years_diff) - 1) * 100

        # Cap growth rate for sanity (prevent extreme outliers)
        annual_growth_rate = max(min(annual_growth_rate, 100), -50)

        # Calculate doubling time using correct formula: ln(2) / ln(1 + r)
        if annual_growth_rate > 0:
            import math

            growth_rate_decimal = annual_growth_rate / 100
            doubling_time = math.log(2) / math.log(1 + growth_rate_decimal)
        else:
            doubling_time = float("inf")

        # Calculate daily accumulation (based on current growth rate)
        # Current value is already in trillions, daily accumulation should be in billions
        current_value_trillions = daily_totals.iloc[-1]["total_wealth"]
        daily_accumulation_billions = (
            (current_value_trillions * annual_growth_rate / 100)
            / 365
            * 1000  # Convert to billions per day
        )

        return {
            "annual_growth_rate": annual_growth_rate,
            "doubling_time": doubling_time,
            "daily_accumulation": daily_accumulation_billions,  # In billions per day
        }

    def _get_monthly_averages(self, daily_totals):
        """Get monthly averages for more stable growth calculations."""
        try:
            # Add year-month column
            daily_totals_copy = daily_totals.copy()
            daily_totals_copy["year_month"] = daily_totals_copy["date"].dt.to_period(
                "M"
            )

            # Calculate monthly averages
            monthly_avg = (
                daily_totals_copy.groupby("year_month")
                .agg(
                    {
                        "total_wealth": "mean",
                        "billionaire_count": "mean",
                        "date": "first",  # Use first date of month as representative
                    }
                )
                .reset_index()
            )

            return monthly_avg.sort_values("date")

        except Exception as e:
            print(f"⚠️  Monthly averaging failed: {e}")
            return pd.DataFrame()  # Return empty DataFrame to trigger fallback

    def load_equivalency_data(self):
        """Load reference values from CSV"""
        csv_path = Path("data/wealth_equivalencies.csv")
        if csv_path.exists():
            df = pd.read_csv(csv_path).set_index("metric")
            print(f"✅ Loaded equivalency data from CSV ({len(df)} metrics)")
            return df
        else:
            print("⚠️ CSV not found, using fallback values")
            return None

    def get_equivalencies(self, total_wealth_trillions):
        """Calculate wealth equivalencies using CSV data."""
        equiv_data = self.load_equivalency_data()
        total_wealth_dollars = (
            total_wealth_trillions * 1e12
        )  # Convert trillions to dollars

        if equiv_data is not None:
            household_income = equiv_data.loc["median_household_income", "value"]
            worker_annual = equiv_data.loc["median_worker_annual", "value"]
            lifetime_earnings = equiv_data.loc["median_lifetime_earnings", "value"]
        else:
            # Fallback to corrected hardcoded values
            household_income, worker_annual, lifetime_earnings = 80610, 59540, 1420000

        return [
            {
                "comparison": "Median US Households",
                "value": f"{total_wealth_dollars / household_income / 1e6:.0f} million",
                "context": "Annual household income",
            },
            {
                "comparison": "Median Workers",
                "value": f"{total_wealth_dollars / worker_annual / 1e6:.0f} million",
                "context": "Annual salaries",
            },
            {
                "comparison": "Average US Workers",
                "value": f"{total_wealth_dollars / lifetime_earnings / 1e6:.0f} million",
                "context": "Lifetime careers",
            },
        ]

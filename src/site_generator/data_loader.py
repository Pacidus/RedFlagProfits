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
        df = pd.read_parquet(self.data_file)
        df["crawl_date"] = pd.to_datetime(df["crawl_date"])
        return df.sort_values("crawl_date")

    def calculate_metrics(self, df):
        """Calculate key metrics from the data."""
        print("📊 Computing dashboard metrics from data...")

        daily_totals = self._compute_daily_totals(df)
        data_start, data_end, data_days, data_points = self._get_data_range(
            daily_totals
        )

        print(
            f"📅 Dataset timespan: {data_start.strftime('%Y-%m-%d')} to "
            f"{data_end.strftime('%Y-%m-%d')} ({data_days} days)"
        )
        print(f"📊 Data collection points: {data_points} days with data")

        # Process current and historical metrics
        latest = daily_totals.iloc[-1]
        first = daily_totals.iloc[0]

        current_metrics = self._compute_current_metrics(latest)
        historical_metrics = self._compute_historical_metrics(first, latest)
        growth_metrics = self._calculate_growth_metrics(daily_totals)

        print(
            f"✅ Metrics computed: {current_metrics['billionaire_count']:,} billionaires, "
            f"${current_metrics['total_wealth_trillions']:.1f}T total"
        )
        print(
            f"📈 Growth: {growth_metrics['growth_rate']:.1f}% CAGR, "
            f"{historical_metrics['wealth_increase_pct']:.1f}% total increase"
        )

        return {
            **current_metrics,
            **growth_metrics,
            **historical_metrics,
            "last_updated": data_end,
            "data_start_date": data_start,
            "data_end_date": data_end,
            "data_days_span": data_days,
            "data_points": data_points,
            "time_series": self._convert_to_trillions(daily_totals),
        }

    def _compute_daily_totals(self, df):
        """Group data by date and calculate daily totals."""
        grouped = (
            df.groupby("crawl_date")
            .agg(
                total_wealth=("finalWorth", "sum"),
                billionaire_count=("personName", "nunique"),
            )
            .reset_index()
        )
        grouped.columns = ["date", "total_wealth", "billionaire_count"]
        return grouped.sort_values("date")

    def _get_data_range(self, daily_totals):
        """Extract dataset date range information."""
        if len(daily_totals) == 0:
            raise ValueError("No data available for calculations")
        start = daily_totals.iloc[0]["date"]
        end = daily_totals.iloc[-1]["date"]
        return (start, end, (end - start).days, len(daily_totals))

    def _compute_current_metrics(self, latest):
        """Calculate metrics from the latest data point."""
        wealth_trillions = latest["total_wealth"] / 1e6
        count = int(latest["billionaire_count"])
        avg_billions = (wealth_trillions * 1e3) / count if count > 0 else 0

        return {
            "total_wealth_trillions": wealth_trillions,
            "billionaire_count": count,
            "average_wealth_billions": avg_billions,
        }

    def _compute_historical_metrics(self, first, latest):
        """Calculate historical comparisons between first and latest data points."""
        fwealth = first["total_wealth"] / 1e6
        lwealth = latest["total_wealth"] / 1e6
        fcount = int(first["billionaire_count"])
        lcount = int(latest["billionaire_count"])

        wealth_pct = self._pct_change(lwealth, fwealth)
        count_diff = lcount - fcount
        avg_pct = self._pct_change(lwealth / lcount, fwealth / fcount)

        return {
            "wealth_increase_pct": wealth_pct,
            "billionaire_increase_count": count_diff,
            "avg_wealth_increase_pct": avg_pct,
        }

    def _pct_change(self, new, old):
        """Calculate percentage change between two values."""
        return ((new - old) / old * 100) if old != 0 else 0

    def _convert_to_trillions(self, df):
        """Convert wealth column to trillions in a DataFrame copy."""
        df_copy = df.copy()
        df_copy["total_wealth"] /= 1_000_000
        return df_copy

    def _calculate_growth_metrics(self, daily_totals):
        """Calculate growth metrics using stable time series approach."""
        if len(daily_totals) < 2:
            return {
                "growth_rate": 0.0,
                "doubling_time": float("inf"),
                "daily_accumulation": 0.0,
            }

        # Use monthly averages if possible
        monthly_data = self._get_monthly_averages(daily_totals)
        use_monthly = len(monthly_data) >= 2

        source = monthly_data if use_monthly else daily_totals
        if not use_monthly and len(daily_totals) >= 2:
            print("⚠️ Using daily data for CAGR (insufficient monthly data)")
        elif use_monthly:
            print(f"✅ Using monthly averages ({len(monthly_data)} months)")

        start_val = source.iloc[0]["total_wealth"]
        end_val = source.iloc[-1]["total_wealth"]
        start_date = source.iloc[0]["date"]
        end_date = source.iloc[-1]["date"]
        days_diff = (end_date - start_date).days

        if days_diff == 0 or start_val == 0:
            return {
                "growth_rate": 0.0,
                "doubling_time": float("inf"),
                "daily_accumulation": 0.0,
            }

        # Calculate CAGR
        years_diff = days_diff / 365.25
        cagr = ((end_val / start_val) ** (1 / years_diff) - 1) * 100
        cagr = max(min(cagr, 100), -50)  # Cap extreme values

        # Calculate doubling time and daily accumulation
        if cagr > 0:
            growth_decimal = cagr / 100
            doubling_time = np.log(2) / np.log(1 + growth_decimal)

            daily_accumulation = (end_val - start_val) / (
                1000 * days_diff
            )  # Billions/day
        else:
            doubling_time = float("inf")
            daily_accumulation = 0.0

        return {
            "growth_rate": cagr,
            "doubling_time": doubling_time,
            "daily_accumulation": daily_accumulation,
        }

    def _get_monthly_averages(self, daily_totals):
        """Compute monthly averages for stable growth calculations."""
        try:
            daily_totals["year_month"] = daily_totals["date"].dt.to_period("M")
            monthly = (
                daily_totals.groupby("year_month")
                .agg(
                    total_wealth=("total_wealth", "mean"),
                    billionaire_count=("billionaire_count", "mean"),
                    date=("date", "first"),
                )
                .reset_index()
            )
            return monthly.sort_values("date")
        except Exception as e:
            print(f"⚠️ Monthly averaging failed: {e}")
            return pd.DataFrame()

    def load_equivalency_data(self):
        """Load reference values from CSV."""
        csv_path = Path("data/wealth_equivalencies.csv")
        if not csv_path.exists():
            print("⚠️ CSV not found, using fallback values")
            return None
        df = pd.read_csv(csv_path).set_index("metric")
        print(f"✅ Loaded equivalency data ({len(df)} metrics)")
        return df

    def get_equivalencies(self, total_wealth_trillions):
        """Calculate wealth equivalencies."""
        equiv_data = self.load_equivalency_data()
        total_dollars = total_wealth_trillions * 1e12

        # Define fallback values
        defaults = {
            "median_household_income": 80610,
            "median_worker_annual": 59540,
            "median_lifetime_earnings": 1_420_000,
        }

        metrics = {
            k: equiv_data.loc[k, "value"] if equiv_data is not None else v
            for k, v in defaults.items()
        }

        results = []
        comparisons = [
            (
                "Median US Households",
                metrics["median_household_income"],
                "Annual household income",
            ),
            ("Median Workers", metrics["median_worker_annual"], "Annual salaries"),
            (
                "Average US Workers",
                metrics["median_lifetime_earnings"],
                "Lifetime careers",
            ),
        ]

        for name, divisor, context in comparisons:
            value = total_dollars / divisor / 1e6
            results.append(
                {
                    "comparison": name,
                    "value": f"{value:.0f} million",
                    "context": context,
                }
            )

        return results

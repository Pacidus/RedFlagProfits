"""Simplified data loading utilities for site generation."""

import pandas as pd
import numpy as np
from pathlib import Path
from data_backend.config import Config


class DataLoader:
    """Loads and processes data for site generation with simplified calculations."""

    def __init__(self, data_file="data/all_billionaires.parquet"):
        self.data_file = Path(data_file)

    def load_latest_data(self):
        """Load the most recent data."""
        df = pd.read_parquet(self.data_file)
        df["crawl_date"] = pd.to_datetime(df["crawl_date"])
        return df.sort_values("crawl_date")

    def calculate_metrics(self, df):
        """Calculate key metrics with consolidated operations."""
        print("📊 Computing dashboard metrics from data...")
        daily_totals = self._compute_daily_totals(df)

        # Extract timespan info
        data_start, data_end = daily_totals.date.iloc[0], daily_totals.date.iloc[-1]
        data_days = (data_end - data_start).days
        data_points = len(daily_totals)

        print(
            f"📅 Dataset: {data_start:%Y-%m-%d} to {data_end:%Y-%m-%d} ({data_days} days)"
        )
        print(f"📊 Collection points: {data_points} days with data")

        # Compute all metrics
        latest, first = daily_totals.iloc[-1], daily_totals.iloc[0]
        metrics = self._compute_all_metrics(first, latest, daily_totals)

        print(
            f"✅ Metrics: {metrics['billionaire_count']:,} billionaires, "
            f"${metrics['total_wealth_trillions']:.1f}T total\n"
            f"📈 Growth: {metrics['growth_rate']:.1f}% CAGR, "
            f"{metrics['wealth_increase_pct']:.1f}% total increase"
        )

        return {
            **metrics,
            "last_updated": data_end,
            "data_start_date": data_start,
            "data_end_date": data_end,
            "data_days_span": data_days,
            "data_points": data_points,
            "time_series": daily_totals.assign(
                total_wealth=lambda x: x.total_wealth / Config.TRILLION
            ),
        }

    def _compute_daily_totals(self, df):
        """Group data by date and calculate daily totals."""
        return (
            df.groupby("crawl_date")
            .agg(
                total_wealth=("finalWorth", "sum"),
                billionaire_count=("personName", "nunique"),
            )
            .reset_index()
            .rename(columns={"crawl_date": "date"})
            .sort_values("date")
        )

    def _compute_all_metrics(self, first, latest, daily_totals):
        """Compute all metrics in one consolidated function."""
        # Current metrics
        count = int(latest.billionaire_count)
        wealth_trillions = latest.total_wealth / Config.TRILLION
        avg_wealth = (wealth_trillions * 1000) / count if count else 0

        # Historical comparisons
        first_wealth = first.total_wealth / Config.TRILLION
        first_count = int(first.billionaire_count)
        first_avg = (first_wealth * 1000) / first_count if first_count else 0

        # Growth calculations
        growth_metrics = self._calculate_growth_metrics(daily_totals)

        return {
            # Current state
            "billionaire_count": count,
            "total_wealth_trillions": wealth_trillions,
            "average_wealth_billions": avg_wealth,
            # Historical changes
            "wealth_increase_pct": self._pct_change(wealth_trillions, first_wealth),
            "billionaire_increase_count": count - first_count,
            "avg_wealth_increase_pct": self._pct_change(avg_wealth, first_avg),
            # Growth metrics
            **growth_metrics,
        }

    def _calculate_growth_metrics(self, daily_totals):
        """Calculate growth metrics with simplified logic."""
        if len(daily_totals) < 2:
            return {
                "growth_rate": 0.0,
                "doubling_time": float("inf"),
                "daily_accumulation": 0.0,
            }

        # Use monthly averages when sufficient data
        source = (
            self._get_monthly_averages(daily_totals)
            if len(daily_totals) > 60
            else daily_totals
        )
        if source is daily_totals:
            print("⚠️ Using daily data for CAGR (insufficient monthly data)")
        else:
            print(f"✅ Using monthly averages ({len(source)} months)")

        start_val, end_val = source.total_wealth.iloc[0], source.total_wealth.iloc[-1]
        days_diff = (source.date.iloc[-1] - source.date.iloc[0]).days

        if not days_diff or not start_val:
            return {
                "growth_rate": 0.0,
                "doubling_time": float("inf"),
                "daily_accumulation": 0.0,
            }

        # Calculate CAGR and derived metrics
        years_diff = days_diff / 365.25
        cagr = np.clip(((end_val / start_val) ** (1 / years_diff) - 1) * 100, -50, 100)

        doubling_time = np.log(2) / np.log(1 + cagr / 100) if cagr > 0 else float("inf")
        daily_accumulation = (
            (end_val - start_val) / (1e3 * days_diff) if cagr > 0 else 0.0
        )

        return {
            "growth_rate": cagr,
            "doubling_time": doubling_time,
            "daily_accumulation": daily_accumulation,
        }

    def _get_monthly_averages(self, daily_totals):
        """Compute monthly averages for stable growth calculations."""
        try:
            return (
                daily_totals.assign(period=lambda x: x.date.dt.to_period("M"))
                .groupby("period")
                .agg(
                    total_wealth=("total_wealth", "mean"),
                    billionaire_count=("billionaire_count", "mean"),
                    date=("date", "first"),
                )
                .sort_values("date")
                .reset_index()
            )
        except Exception as e:
            print(f"⚠️ Monthly averaging failed: {e}")
            return daily_totals

    def get_equivalencies(self, total_wealth_trillions):
        """Calculate wealth equivalencies with simplified logic."""
        # Load or use default metrics
        csv_path = Path("data/wealth_equivalencies.csv")
        if csv_path.exists():
            metrics = pd.read_csv(csv_path).set_index("metric").value
            print(f"✅ Loaded equivalency data ({len(metrics)} metrics)")
        else:
            metrics = Config.DEFAULT_METRICS
            print("⚠️ CSV not found, using fallback values")

        # Calculate equivalencies
        total_dollars = total_wealth_trillions * 1e12
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

        return [
            {
                "comparison": name,
                "value": f"{total_dollars / divisor / 1e6:.0f} million",
                "context": context,
            }
            for name, divisor, context in comparisons
        ]

    @staticmethod
    def _pct_change(new, old):
        """Calculate percentage change between two values."""
        return ((new - old) / old * 100) if old else 0

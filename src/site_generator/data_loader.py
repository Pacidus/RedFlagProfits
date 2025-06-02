"""Data loading utilities for site generation."""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path


class DataLoader:
    """Loads and processes data for site generation."""

    TRILLION = 1_000_000  # Conversion factor: millions → trillions

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

        # Extract dataset date range
        data_start, data_end = daily_totals.date.iloc[0], daily_totals.date.iloc[-1]
        data_days = (data_end - data_start).days
        data_points = len(daily_totals)

        print(
            f"📅 Dataset timespan: {data_start:%Y-%m-%d} to {data_end:%Y-%m-%d} ({data_days} days)"
        )
        print(f"📊 Data collection points: {data_points} days with data")

        # Process metrics
        latest, first = daily_totals.iloc[-1], daily_totals.iloc[0]
        current = self._compute_current_metrics(latest)
        historical = self._compute_historical_metrics(first, latest)
        growth = self._calculate_growth_metrics(daily_totals)

        print(
            f"✅ Metrics computed: {current['billionaire_count']:,} billionaires, "
            f"${current['total_wealth_trillions']:.1f}T total\n"
            f"📈 Growth: {growth['growth_rate']:.1f}% CAGR, "
            f"{historical['wealth_increase_pct']:.1f}% total increase"
        )

        return {
            **current,
            **growth,
            **historical,
            "last_updated": data_end,
            "data_start_date": data_start,
            "data_end_date": data_end,
            "data_days_span": data_days,
            "data_points": data_points,
            "time_series": daily_totals.assign(
                total_wealth=lambda x: x.total_wealth / self.TRILLION
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

    def _compute_current_metrics(self, latest):
        """Calculate metrics from the latest data point."""
        wealth_trillions = latest.total_wealth / self.TRILLION
        count = int(latest.billionaire_count)
        avg_billions = (wealth_trillions * 1000) / count if count else 0

        return {
            "total_wealth_trillions": wealth_trillions,
            "billionaire_count": count,
            "average_wealth_billions": avg_billions,
        }

    def _compute_historical_metrics(self, first, latest):
        """Calculate historical comparisons."""

        def to_trillions(x):
            return x.total_wealth / self.TRILLION

        fwealth, lwealth = to_trillions(first), to_trillions(latest)
        fcount, lcount = int(first.billionaire_count), int(latest.billionaire_count)

        wealth_pct = self._pct_change(lwealth, fwealth)
        count_diff = lcount - fcount
        avg_pct = self._pct_change(lwealth / lcount, fwealth / fcount) if fcount else 0

        return {
            "wealth_increase_pct": wealth_pct,
            "billionaire_increase_count": count_diff,
            "avg_wealth_increase_pct": avg_pct,
        }

    def _pct_change(self, new, old):
        """Calculate percentage change between two values."""
        return ((new - old) / old * 100) if old else 0

    def _calculate_growth_metrics(self, daily_totals):
        """Calculate growth metrics using stable time series approach."""
        if len(daily_totals) < 2:
            return {
                "growth_rate": 0.0,
                "doubling_time": float("inf"),
                "daily_accumulation": 0.0,
            }

        # Use monthly averages when possible
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

        # Calculate CAGR
        years_diff = days_diff / 365.25
        cagr = ((end_val / start_val) ** (1 / years_diff) - 1) * 100
        cagr = np.clip(cagr, -50, 100)  # Cap extreme values

        # Calculate additional metrics
        if cagr > 0:
            doubling_time = np.log(2) / np.log(1 + cagr / 100)
            daily_accumulation = (end_val - start_val) / (
                1000 * days_diff
            )  # Billions/day
        else:
            doubling_time, daily_accumulation = float("inf"), 0.0

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
            return daily_totals  # Fallback to daily data

    def get_equivalencies(self, total_wealth_trillions):
        """Calculate wealth equivalencies."""
        DEFAULT_METRICS = {
            "median_household_income": 80610,
            "median_worker_annual": 59540,
            "median_lifetime_earnings": 1_420_000,
        }

        # Load or use default metrics
        csv_path = Path("data/wealth_equivalencies.csv")
        if csv_path.exists():
            metrics = pd.read_csv(csv_path).set_index("metric").value
            print(f"✅ Loaded equivalency data ({len(metrics)} metrics)")
        else:
            metrics = DEFAULT_METRICS
            print("⚠️ CSV not found, using fallback values")

        # Prepare comparisons
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

        # Calculate equivalencies
        total_dollars = total_wealth_trillions * 1e12
        return [
            {
                "comparison": name,
                "value": f"{total_dollars / divisor / 1e6:.0f} million",
                "context": context,
            }
            for name, divisor, context in comparisons
        ]  # return results

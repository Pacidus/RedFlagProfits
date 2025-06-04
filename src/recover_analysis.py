#!/usr/bin/env python3
"""
Data Gap Analysis Utility

Analyzes the current dataset to identify gaps and potential recovery opportunities.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from pathlib import Path
import requests

from data_backend import Config


class DataGapAnalyzer:
    """Analyzes data gaps and recovery opportunities."""

    def __init__(self):
        self.data_file = Config.PARQUET_FILE

    def load_current_data(self):
        """Load and analyze current dataset."""
        if not self.data_file.exists():
            print("❌ No existing dataset found")
            return None

        data = pd.read_parquet(self.data_file)
        data["crawl_date"] = pd.to_datetime(data["crawl_date"])
        return data

    def analyze_date_coverage(self, data):
        """Analyze date coverage and identify gaps."""
        if data is None:
            return

        print("📊 CURRENT DATASET ANALYSIS")
        print("=" * 50)

        # Basic stats
        date_range = data.groupby("crawl_date").size()

        print(
            f"📅 Date Range: {date_range.index.min().date()} to {date_range.index.max().date()}"
        )
        print(f"📊 Total Days with Data: {len(date_range)}")
        print(f"👥 Total Records: {len(data):,}")

        # Identify gaps
        full_range = pd.date_range(
            start=date_range.index.min(), end=date_range.index.max(), freq="D"
        )

        missing_dates = full_range.difference(date_range.index)

        print(f"\n🕳️  MISSING DATES")
        print("=" * 30)
        print(f"Missing Days: {len(missing_dates)}")
        print(f"Coverage: {(1 - len(missing_dates)/len(full_range)) * 100:.1f}%")

        if len(missing_dates) > 0:
            # Group consecutive missing dates
            gaps = self._find_consecutive_gaps(missing_dates)

            print(f"\n📊 GAP ANALYSIS")
            print("=" * 30)
            print(f"Number of Gaps: {len(gaps)}")

            # Show largest gaps
            gaps_sorted = sorted(gaps, key=lambda x: len(x), reverse=True)
            print(f"\n🔍 LARGEST GAPS:")
            for i, gap in enumerate(gaps_sorted[:5], 1):
                print(f"  {i}. {gap[0].date()} to {gap[-1].date()} ({len(gap)} days)")

        return missing_dates

    def _find_consecutive_gaps(self, missing_dates):
        """Find consecutive date gaps."""
        if len(missing_dates) == 0:
            return []

        gaps = []
        current_gap = [missing_dates[0]]

        for i in range(1, len(missing_dates)):
            if missing_dates[i] - missing_dates[i - 1] == timedelta(days=1):
                current_gap.append(missing_dates[i])
            else:
                gaps.append(current_gap)
                current_gap = [missing_dates[i]]

        gaps.append(current_gap)
        return gaps

    def check_wayback_availability(self, sample_dates=5):
        """Check if Wayback Machine has data for missing dates."""
        print(f"\n🔍 WAYBACK MACHINE AVAILABILITY CHECK")
        print("=" * 50)

        # Quick check for a few sample dates
        cdx_api = "https://web.archive.org/cdx/search/cdx"
        forbes_url = "https://www.forbes.com/forbesapi/person/rtb/0/position/true.json"

        params = {
            "url": forbes_url,
            "output": "json",
            "filter": ["statuscode:200", "mimetype:application/json"],
            "collapse": "timestamp:8",
        }

        try:
            response = requests.get(cdx_api, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            if data and len(data) > 1:
                # Parse snapshots
                headers = data[0]
                snapshots = []
                for row in data[1:]:
                    snapshot = dict(zip(headers, row))
                    try:
                        dt = pd.to_datetime(
                            snapshot["timestamp"], format="%Y%m%d%H%M%S"
                        )
                        snapshots.append(dt.date())
                    except:
                        continue

                print(f"✅ Found {len(snapshots)} snapshots in Wayback Machine")

                if snapshots:
                    earliest = min(snapshots)
                    latest = max(snapshots)
                    print(f"📅 Wayback Range: {earliest} to {latest}")

                    # Estimate potential recovery
                    wayback_dates = set(snapshots)
                    print(f"📊 Unique dates available: {len(wayback_dates)}")

            else:
                print("⚠️  No snapshots found in Wayback Machine")

        except Exception as e:
            print(f"❌ Failed to check Wayback Machine: {e}")

    def generate_recovery_plan(self, missing_dates):
        """Generate a recovery plan."""
        if missing_dates is None or len(missing_dates) == 0:
            print("\n✅ No data gaps found - no recovery needed!")
            return

        print(f"\n📋 RECOVERY PLAN")
        print("=" * 30)

        # Suggest recovery strategy
        total_missing = len(missing_dates)

        if total_missing <= 30:
            print("🎯 Strategy: Full Recovery")
            print(f"   Recommended: Recover all {total_missing} missing dates")
        elif total_missing <= 100:
            print("🎯 Strategy: Prioritized Recovery")
            print(f"   Recommended: Focus on recent gaps first")
        else:
            print("🎯 Strategy: Strategic Recovery")
            print(f"   Recommended: Sample key dates or focus on specific periods")

        print(f"\n📝 COMMANDS TO RUN:")
        print("=" * 30)

        earliest = min(missing_dates).strftime("%Y-%m-%d")
        latest = max(missing_dates).strftime("%Y-%m-%d")

        print("1. Dry run to see what's available:")
        print(
            f"   python recover_historical_data.py --start-date {earliest} --end-date {latest} --dry-run"
        )

        print("\n2. Full recovery:")
        print(
            f"   python recover_historical_data.py --start-date {earliest} --end-date {latest}"
        )

        print("\n3. Conservative recovery (recent data only):")
        recent_start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        print(f"   python recover_historical_data.py --start-date {recent_start}")

    def create_visualization(self, data):
        """Create a visualization of data coverage."""
        if data is None:
            return

        try:
            # Create coverage plot
            plt.style.use("dark_background")
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
            fig.patch.set_facecolor("#1a1a1a")

            # Daily record counts
            daily_counts = data.groupby("crawl_date").size()
            ax1.plot(
                daily_counts.index, daily_counts.values, color="#e74c3c", linewidth=1.5
            )
            ax1.set_title(
                "Daily Data Collection", color="#f8f9fa", fontsize=14, fontweight="bold"
            )
            ax1.set_ylabel("Records per Day", color="#adb5bd")
            ax1.grid(True, alpha=0.3)
            ax1.set_facecolor("#2d2d2d")

            # Coverage heatmap (by month)
            data["year_month"] = data["crawl_date"].dt.to_period("M")
            monthly_coverage = data.groupby("year_month").size()

            # Create monthly coverage plot
            ax2.bar(
                range(len(monthly_coverage)),
                monthly_coverage.values,
                color="#e74c3c",
                alpha=0.7,
            )
            ax2.set_title(
                "Monthly Data Coverage", color="#f8f9fa", fontsize=14, fontweight="bold"
            )
            ax2.set_ylabel("Records", color="#adb5bd")
            ax2.set_xlabel("Time Period", color="#adb5bd")
            ax2.grid(True, alpha=0.3)
            ax2.set_facecolor("#2d2d2d")

            # Set tick labels for monthly plot
            tick_positions = range(
                0, len(monthly_coverage), max(1, len(monthly_coverage) // 10)
            )
            ax2.set_xticks(tick_positions)
            ax2.set_xticklabels(
                [str(monthly_coverage.index[i]) for i in tick_positions],
                rotation=45,
                color="#adb5bd",
            )

            plt.tight_layout()
            plt.savefig(
                "data_coverage_analysis.png",
                dpi=150,
                bbox_inches="tight",
                facecolor="#1a1a1a",
            )
            print(f"\n📊 Visualization saved as 'data_coverage_analysis.png'")

        except Exception as e:
            print(f"⚠️  Could not create visualization: {e}")


def main():
    """Main analysis function."""
    analyzer = DataGapAnalyzer()

    print("🔍 RED FLAGS PROFITS - DATA GAP ANALYSIS")
    print("=" * 60)

    # Load and analyze current data
    data = analyzer.load_current_data()
    missing_dates = analyzer.analyze_date_coverage(data)

    # Check Wayback Machine availability
    analyzer.check_wayback_availability()

    # Generate recovery plan
    analyzer.generate_recovery_plan(missing_dates)

    # Create visualization
    analyzer.create_visualization(data)

    print(f"\n🎉 Analysis complete!")


if __name__ == "__main__":
    main()

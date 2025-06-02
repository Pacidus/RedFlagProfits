"""Static site generator for Red Flags Profits website."""

import os
import json
import pandas as pd
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from datetime import datetime
import shutil

from .chart_data_processor import ChartDataProcessor
from .background_sparklines import BackgroundSparklineGenerator
from .data_loader import DataLoader
from .config import SiteConfig


class RedFlagsSiteGenerator:
    """Static site generator for Red Flags Profits website."""

    def __init__(
        self, template_dir="src/templates", static_dir="src/static", output_dir="docs"
    ):
        self.template_dir = Path(template_dir)
        self.static_dir = Path(static_dir)
        self.output_dir = Path(output_dir)
        self.data_loader = DataLoader()

        # Setup Jinja2 environment
        self.env = Environment(
            loader=FileSystemLoader(self.template_dir),
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Add custom filters
        self.env.filters["currency"] = self.format_currency
        self.env.filters["number"] = self.format_number
        self.env.filters["percentage"] = self.format_percentage
        self.env.filters["date"] = self.format_date

    def generate_site(self, data):
        """Generate the complete static site."""
        print("🏗️  Generating Red Flags Profits website...")

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Copy static assets
        self.copy_static_files()

        # Calculate metrics from data - FRESH COMPUTATION EACH TIME
        print("🔄 Computing fresh metrics from loaded data...")
        dashboard_data = self.data_loader.calculate_metrics(data)
        analysis_data = self.prepare_analysis_data(dashboard_data)

        # NEW: Prepare chart data
        print("📊 Preparing chart data...")
        chart_data = self.prepare_chart_data(dashboard_data)

        # Add chart data to dashboard data for template access
        dashboard_data["charts"] = chart_data

        # Generate background sparklines
        print("✨ Generating background sparklines...")
        sparkline_gen = BackgroundSparklineGenerator()
        background_sparklines = sparkline_gen.generate_all_backgrounds(dashboard_data)

        # Add sparklines to dashboard data for template access
        dashboard_data["background_sparklines"] = background_sparklines

        # Generate single page with everything
        self.generate_index(dashboard_data, analysis_data)

        print("✅ Site generation complete!")
        print(f"📁 Output: {self.output_dir.absolute()}")
        print(
            f"📊 Data timespan: {dashboard_data['data_start_date'].strftime('%Y-%m-%d')} to {dashboard_data['data_end_date'].strftime('%Y-%m-%d')} ({dashboard_data['data_days_span']} days)"
        )
        print(f"📈 Collection points: {dashboard_data['data_points']} days with data")

    def copy_static_files(self):
        """Copy CSS, JS, and assets to output directory."""
        for subdir in ["css", "js", "assets"]:
            src_dir = self.static_dir / subdir
            dst_dir = self.output_dir / subdir

            if src_dir.exists():
                # Remove existing and copy fresh
                if dst_dir.exists():
                    shutil.rmtree(dst_dir)
                shutil.copytree(src_dir, dst_dir)

    def generate_index(self, dashboard_data, analysis_data):
        """Generate single comprehensive page."""
        template = self.env.get_template("index.html")

        # Render template with fresh data
        html_content = template.render(
            page_title="Red Flags Profits - Wealth Monopolization Analysis",
            dashboard=dashboard_data,
            analysis=analysis_data,
            last_updated=datetime.now().strftime("%Y-%m-%d %H:%M UTC"),
        )

        # Write to file
        with open(self.output_dir / "index.html", "w", encoding="utf-8") as f:
            f.write(html_content)

        print(f"📄 Generated index.html with {len(html_content):,} characters")

    def prepare_analysis_data(self, dashboard_data):
        """Prepare data for analysis page."""
        # Get wealth equivalencies - pass the correct value in trillions
        equivalencies = self.data_loader.get_equivalencies(
            dashboard_data["total_wealth_trillions"]  # FIXED: Use the trillions value
        )

        return {
            "wealth_equivalencies": equivalencies,
            "growth_metrics": {
                "real_growth_rate": dashboard_data["growth_rate"],
                "inflation_adjusted": True,
                "acceleration": (
                    "increasing" if dashboard_data["growth_rate"] > 8.0 else "stable"
                ),
            },
        }

    def prepare_chart_data(self, dashboard_data):
        """Prepare all chart data for JavaScript consumption."""
        chart_processor = ChartDataProcessor()  # No logger needed

        # Prepare wealth timeline chart data
        time_series_df = dashboard_data.get("time_series")
        current_cpi = dashboard_data.get("cpi_u")  # From current data
        base_cpi = None  # We could get this from the first data point if needed

        if time_series_df is not None and len(time_series_df) > 0:
            # Try to get base CPI from the earliest data point for inflation adjustment
            if "cpi_u" in time_series_df.columns:
                base_cpi = (
                    time_series_df["cpi_u"].iloc[0]
                    if not time_series_df["cpi_u"].isna().all()
                    else None
                )

        wealth_timeline_data = chart_processor.prepare_wealth_timeline_data(
            time_series_df
        )

        # Export chart data to static files for JavaScript
        chart_data_dir = self.output_dir / "js" / "data"
        chart_data_dir.mkdir(parents=True, exist_ok=True)

        chart_processor.export_chart_data_to_json(
            wealth_timeline_data, chart_data_dir / "wealth_timeline.json"
        )

        print("✅ Chart data preparation complete")

        return {"wealth_timeline": wealth_timeline_data}

    # FIXED: Jinja2 custom filters - now handle the correct units
    @staticmethod
    def format_currency(value, symbol="$", precision=1):
        """Format currency values with appropriate scale."""
        # Value is expected to be in the unit it should be displayed as
        # No additional conversion needed
        return f"{symbol}{value:.{precision}f}"

    @staticmethod
    def format_number(value, precision=0):
        """Format large numbers with commas."""
        return f"{value:,.{precision}f}"

    @staticmethod
    def format_percentage(value, precision=1):
        """Format percentage values."""
        return f"{value:+.{precision}f}%"

    @staticmethod
    def format_date(date_obj, format_str="%B %d, %Y"):
        """Format date objects."""
        if isinstance(date_obj, str):
            date_obj = datetime.fromisoformat(date_obj)
        return date_obj.strftime(format_str)

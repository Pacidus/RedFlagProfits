"""Static site generator for Red Flags Profits website."""

import os
import json
import pandas as pd
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from datetime import datetime
import shutil

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

        # NEW: Generate background sparklines
        print("✨ Generating background sparklines...")
        sparkline_gen = BackgroundSparklineGenerator()
        background_sparklines = sparkline_gen.generate_all_background_sparklines(
            dashboard_data
        )

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
        # Get wealth equivalencies
        equivalencies = self.data_loader.get_equivalencies(
            dashboard_data["total_wealth"]
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

    # Jinja2 custom filters
    @staticmethod
    def format_currency(value, symbol="$", precision=1):
        """Format currency values with appropriate scale."""
        if value >= 1000:
            return f"{symbol}{value:.{precision}f}T"
        elif value >= 1:
            return f"{symbol}{value:.{precision}f}B"
        else:
            return f"{symbol}{value*1000:.0f}M"

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

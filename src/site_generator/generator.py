"""Simplified static site generator for Red Flags Profits website."""

import shutil
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from datetime import datetime

from .chart_data_processor import ChartDataProcessor
from .background_sparklines import BackgroundSparklineGenerator
from .data_loader import DataLoader


class RedFlagsSiteGenerator:
    """Static site generator with simplified operations."""

    def __init__(
        self, template_dir="src/templates", static_dir="src/static", output_dir="docs"
    ):
        self.template_dir = Path(template_dir)
        self.static_dir = Path(static_dir)
        self.output_dir = Path(output_dir)
        self.data_loader = DataLoader()

        # Setup Jinja2 with custom filters
        self.env = Environment(
            loader=FileSystemLoader(self.template_dir),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self._add_custom_filters()

    def generate_site(self, data):
        """Generate the complete static site with consolidated operations."""
        print("🏗️  Generating Red Flags Profits website...")

        # Create output and copy static files
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._copy_static_files()

        # Compute all data and generate components
        dashboard_data = self._prepare_all_data(data)

        # Generate single comprehensive page
        self._generate_index(dashboard_data)

        print("✅ Site generation complete!")
        print(f"📁 Output: {self.output_dir.absolute()}")
        print(
            f"📊 Data: {dashboard_data['data_start_date']:%Y-%m-%d} to {dashboard_data['data_end_date']:%Y-%m-%d} ({dashboard_data['data_days_span']} days)"
        )

    def _prepare_all_data(self, data):
        """Prepare all data in one consolidated operation."""
        print("🔄 Computing fresh metrics from loaded data...")
        dashboard_data = self.data_loader.calculate_metrics(data)

        # Add chart data
        print("📊 Preparing chart data...")
        chart_processor = ChartDataProcessor()
        chart_data = {
            "wealth_timeline": chart_processor.prepare_wealth_timeline_data(
                dashboard_data.get("time_series")
            )
        }
        dashboard_data["charts"] = chart_data

        # Export chart data
        chart_data_dir = self.output_dir / "js" / "data"
        chart_data_dir.mkdir(parents=True, exist_ok=True)
        chart_processor.export_chart_data_to_json(
            chart_data["wealth_timeline"], chart_data_dir / "wealth_timeline.json"
        )

        # Add background sparklines
        print("✨ Generating background sparklines...")
        sparkline_gen = BackgroundSparklineGenerator()
        dashboard_data["background_sparklines"] = (
            sparkline_gen.generate_all_backgrounds(dashboard_data)
        )

        # Add analysis data
        dashboard_data["analysis"] = {
            "wealth_equivalencies": self.data_loader.get_equivalencies(
                dashboard_data["total_wealth_trillions"]
            ),
            "growth_metrics": {
                "real_growth_rate": dashboard_data["growth_rate"],
                "inflation_adjusted": True,
                "acceleration": (
                    "increasing" if dashboard_data["growth_rate"] > 8.0 else "stable"
                ),
            },
        }

        return dashboard_data

    def _copy_static_files(self):
        """Copy CSS, JS, and assets to output directory."""
        for subdir in ["css", "js", "assets"]:
            src_dir = self.static_dir / subdir
            dst_dir = self.output_dir / subdir
            if src_dir.exists():
                if dst_dir.exists():
                    shutil.rmtree(dst_dir)
                shutil.copytree(src_dir, dst_dir)

    def _generate_index(self, dashboard_data):
        """Generate single comprehensive page."""
        template = self.env.get_template("index.html")

        html_content = template.render(
            page_title="Red Flags Profits - Wealth Monopolization Analysis",
            dashboard=dashboard_data,
            analysis=dashboard_data["analysis"],
            last_updated=datetime.now().strftime("%Y-%m-%d %H:%M UTC"),
        )

        with open(self.output_dir / "index.html", "w", encoding="utf-8") as f:
            f.write(html_content)

        print(f"📄 Generated index.html with {len(html_content):,} characters")

    def _add_custom_filters(self):
        """Add custom Jinja2 filters."""
        filter_map = {
            "currency": lambda value, symbol="$", precision=1: f"{symbol}{value:.{precision}f}",
            "number": lambda value, precision=0: f"{value:,.{precision}f}",
            "percentage": lambda value, precision=1: f"{value:+.{precision}f}%",
            "date": lambda date_obj, format_str="%B %d, %Y": (
                datetime.fromisoformat(date_obj)
                if isinstance(date_obj, str)
                else date_obj
            ).strftime(format_str),
        }

        for name, func in filter_map.items():
            self.env.filters[name] = func

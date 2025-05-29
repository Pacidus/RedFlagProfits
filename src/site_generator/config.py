"""Configuration for site generation."""

from pathlib import Path


class SiteConfig:
    """Site generation configuration."""

    # Paths
    TEMPLATE_DIR = Path("src/templates")
    STATIC_DIR = Path("src/static")
    OUTPUT_DIR = Path("docs")

    # Site metadata
    SITE_NAME = "Red Flags Profits"
    SITE_DESCRIPTION = "Wealth Monopolization Analysis"
    SITE_URL = "https://username.github.io/red-flags-profits"  # Update with your GitHub username

    # Chart configuration
    CHART_COLORS = {
        "primary": "#e74c3c",
        "secondary": "#c0392b",
        "accent": "#ff6b6b",
        "background": "#1a1a1a",
    }

    # Reference values for calculations
    MEDIAN_HOUSEHOLD_INCOME = 70000
    MINIMUM_WAGE_ANNUAL = 15000
    PROFESSIONAL_SALARY = 80000

#!/usr/bin/env python3
"""
Red Flags Profits - Static Site Generator

Generates the static website from data and templates.
"""

import sys
import pandas as pd
from pathlib import Path

# Add src to path to import site_generator
sys.path.insert(0, str(Path(__file__).parent))

from site_generator.generator import RedFlagsSiteGenerator


def main():
    """Generate the complete static site."""
    print("🏗️  Starting Red Flags Profits site generation...")

    try:
        # Load the latest data
        data_file = Path("data/all_billionaires.parquet")
        if not data_file.exists():
            print("❌ No data file found. Run update_data.py first.")
            return False

        data = pd.read_parquet(data_file)
        print(f"✅ Loaded {len(data):,} records")

        # Initialize site generator
        generator = RedFlagsSiteGenerator(
            template_dir="src/templates",
            static_dir="src/static",
            output_dir="docs",  # GitHub Pages
        )

        # Generate the site
        generator.generate_site(data)

        print("🎉 Site generation completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Site generation failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

#!/usr/bin/env python3
"""
Main Analysis Workflow for RedFlagProfits

This script orchestrates the entire data processing and visualization workflow:
1. Fetch new data and update the parquet dataset (optional)
2. Process the data
3. Generate visualizations
4. Update the README

Usage:
    python run_analysis.py [--skip-update] [--show-plots]
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime


def check_dependencies():
    """Check if all required dependencies are installed."""
    required_packages = [
        "numpy",
        "pandas",
        "polars",
        "matplotlib",
        "plotly",
        "pyarrow",
        "requests",
        "zstandard",  # zstandard for compression
    ]

    missing_packages = []

    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        print("Missing required dependencies:")
        for package in missing_packages:
            print(f"  - {package}")
        print("\nPlease install the missing dependencies with:")
        print(f"pip install {' '.join(missing_packages)}")
        return False

    return True


def update_data():
    """Fetch new data and update the parquet dataset."""
    print("\n=== Updating Data ===")

    # Create data directories if they don't exist
    os.makedirs("data", exist_ok=True)
    os.makedirs("data/dictionaries", exist_ok=True)

    try:
        # Import and run the update function
        from update_parquet_data import main as update_main

        success = update_main()

        if not success:
            print("Warning: Data update failed or no new data available.")
            print("Continuing with existing data...")
        else:
            print("Data updated successfully")

        return True
    except Exception as e:
        print(f"Error updating data: {e}")
        print("Continuing with existing data...")
        return False


def generate_visualizations(show_plots=False):
    """Process data and generate visualizations."""
    print("\n=== Generating Visualizations ===")

    # Create output directory
    os.makedirs("docs/figures", exist_ok=True)

    try:
        # Import and run the visualization function
        from make_figure_parquet import main as viz_main

        # Process billionaires (worth >= $1 billion)
        viz_main(
            1000, "docs/figures/Billionaires", r"Billionaires ($\geq$ 1B)", show_plots
        )

        # Process millionaires (worth >= $1 million)
        viz_main(
            1, "docs/figures/Millionaires", r"Millionaires ($\geq$ 1M)", show_plots
        )

        print("Visualizations generated successfully")
        return True
    except Exception as e:
        print(f"Error generating visualizations: {e}")
        return False


def update_readme():
    """Update the README with the current timestamp."""
    print("\n=== Updating README ===")

    try:
        with open("README.md", "r") as f:
            readme_content = f.read()

        # Replace the last line with current timestamp
        current_time = datetime.utcnow().strftime("%a %b %d %H:%M:%S UTC %Y")
        readme_lines = readme_content.split("\n")

        if "Last run:" in readme_lines[-1]:
            readme_lines[-1] = f"Last run: {current_time}"
        else:
            readme_lines.append(f"Last run: {current_time}")

        with open("README.md", "w") as f:
            f.write("\n".join(readme_lines))

        print(f"README updated with timestamp: {current_time}")
        return True
    except Exception as e:
        print(f"Error updating README: {e}")
        return False


def main():
    """Main function to orchestrate the workflow."""
    parser = argparse.ArgumentParser(
        description="Run the RedFlagProfits analysis workflow"
    )
    parser.add_argument(
        "--skip-update", action="store_true", help="Skip data update step"
    )
    parser.add_argument(
        "--show-plots", action="store_true", help="Show plots during visualization"
    )

    args = parser.parse_args()

    print("=== RedFlagProfits Analysis Workflow ===")
    print(f"Starting at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Check dependencies
    print("\n=== Checking Dependencies ===")
    if not check_dependencies():
        print("Error: Missing required dependencies. Exiting.")
        return 1

    # Update data (unless skipped)
    if not args.skip_update:
        if not update_data():
            print("Warning: Data update step encountered issues.")
    else:
        print("\n=== Skipping Data Update ===")

    # Generate visualizations
    if not generate_visualizations(args.show_plots):
        print("Error: Visualization generation failed. Exiting.")
        return 1

    # Update README
    if not update_readme():
        print("Warning: README update failed.")

    print("\n=== Analysis Workflow Completed Successfully ===")
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

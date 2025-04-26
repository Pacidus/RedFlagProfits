# RedFlagProfits Parquet Workflow

This document describes the updated workflow for the RedFlagProfits project that uses an optimized Parquet dataset instead of CSV files.

## Overview

The original CSV-based workflow has been replaced with a more efficient Parquet-based system. The Parquet format offers several advantages:

- **Reduced file size**: Compressed storage with optimized columnar format
- **Faster processing**: Column-oriented storage for better query performance
- **Schema enforcement**: Consistent data types and structure
- **Built-in statistics**: Metadata for improved query planning
- **Efficient handling of complex data**: Better storage of nested structures

## File Structure

- **data/all_billionaires.parquet**: Main dataset containing all billionaire data
- **data/dictionaries/**: Mapping dictionaries for decoding encoded values
  - exchanges.json: Exchange code to name mapping
  - currencies.json: Currency code to name mapping
  - industries.json: Industry code to name mapping
  - companies.json: Company code to name mapping

## Scripts

### Core Workflow Scripts

1. **update_data.py**: Fetches new data from Forbes API and updates the Parquet dataset
   ```
   python update_parquet_data.py
   ```

2. **make_figure_parquet.py**: Processes the Parquet data and generates visualizations
   ```
   python make_figure_parquet.py [show]
   ```

3. **run_analysis.py**: Orchestrates the entire workflow from data fetching to visualization
   ```
   python run_analysis.py [--skip-update] [--show-plots]
   ```

### Utility Scripts

1. **explore_parquet.py**: Provides utilities for exploring and analyzing the Parquet dataset
   ```
   python explore_parquet.py --help
   
   # Examples:
   python explore_parquet.py --analyze
   python explore_parquet.py --billionaire "Elon Musk"
   python explore_parquet.py --top 20
   python explore_parquet.py --distribution
   python explore_parquet.py --industries
   ```

2. **data_check.py**: Tests reading from the Parquet file and creates test visualizations

## Migration from CSV to Parquet

The original dataset has been converted to Parquet format using the `convert_parquet.py` script. This conversion includes:

- Normalizing string values to integer codes for efficient storage
- Decomposing complex nested structures (e.g., financialAssets) into columnar format
- Optimizing compression settings for maximum space savings

## Usage Guide

### Running the Complete Workflow

The simplest way to run the entire workflow is using the `run_analysis.py` script:

```bash
# Run the full workflow (update data + generate visualizations)
python run_analysis.py

# Skip data update and only regenerate visualizations
python run_analysis.py --skip-update

# Show plots as they're generated
python run_analysis.py --show-plots
```

### Exploring the Data

Use the `explore_parquet.py` script to analyze the dataset:

```bash
# Show basic analysis of the Parquet file structure
python explore_parquet.py --analyze

# Look up a specific billionaire
python explore_parquet.py --billionaire "Jeff Bezos"

# Show the top 50 billionaires
python explore_parquet.py --top 50

# Show wealth distribution
python explore_parquet.py --distribution

# Analyze industry distribution
python explore_parquet.py --industries

# Analyze a specific date
python explore_parquet.py --date "2023-01-15" --top 20
```

## Requirements

The following Python packages are required:

- numpy
- pandas
- polars
- matplotlib
- plotly
- pyarrow
- requests
- zstandard (for compression)

Install them using:

```bash
pip install numpy pandas polars matplotlib plotly pyarrow requests zstandard
```

## Implementation Notes

1. **Data Encoding**: String values (exchange names, currencies, industries, etc.) are encoded as integer codes to reduce storage space and improve performance.

2. **Complex Data Handling**: Nested structures like financial assets are decomposed into separate column arrays for better columnar storage efficiency.

3. **Data Validation**: The validation process ensures that the Parquet data matches the original CSV data with high accuracy (99.48% sample validation rate, 100% aggregate validation).

4. **Timestamp Handling**: Dates are stored as proper datetime objects rather than strings, allowing for more efficient filtering and aggregation.

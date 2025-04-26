#!/usr/bin/env python3
"""
Comprehensive Parquet Validation Script for RedFlagProfits

This script performs thorough validation between the original CSV data
and the new Parquet format to ensure data consistency and integrity
before the original data can be safely deleted.

Updated to handle missing date_str column.
"""

import pandas as pd
import os
import json
import glob
import logging
import ast
from datetime import datetime
import matplotlib.pyplot as plt
from tqdm import tqdm
import random

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("validation.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Paths
CSV_DIR = "data"
PARQUET_FILE = "data_parquet/all_billionaires.parquet"
DICT_PATH = "data_parquet/dictionaries"
VALIDATION_DIR = "validation_results"


# Load dictionaries for decoding
def load_dictionaries():
    """Load mapping dictionaries used during conversion."""
    dicts = {}

    for dict_name in ["exchanges", "currencies", "industries", "companies"]:
        dict_file = os.path.join(DICT_PATH, f"{dict_name}.json")
        if os.path.exists(dict_file):
            with open(dict_file, "r") as f:
                dicts[dict_name] = json.load(f)
                # Create reverse mapping
                dicts[f"{dict_name}_rev"] = {v: k for k, v in dicts[dict_name].items()}

    return dicts


# Parse complex fields in CSV
def parse_complex_field(field_value):
    """Parse complex fields from string representation."""
    if pd.isna(field_value) or field_value == "":
        return []

    try:
        return ast.literal_eval(field_value)
    except (ValueError, SyntaxError):
        try:
            return json.loads(field_value)
        except json.JSONDecodeError:
            return field_value


# Count validation function
def validate_record_counts():
    """Validate that all records from CSVs are in the parquet file."""
    logger.info("Validating record counts...")

    # Get all CSV files
    csv_files = glob.glob(os.path.join(CSV_DIR, "*.csv"))

    # Count total records in CSVs
    total_csv_records = 0
    date_record_counts = {}

    for csv_file in tqdm(csv_files, desc="Counting CSV records"):
        date = os.path.basename(csv_file).split(".")[0]
        df = pd.read_csv(csv_file)
        record_count = len(df)
        total_csv_records += record_count
        date_record_counts[date] = record_count

    # Count records in parquet by date
    parquet_df = pd.read_parquet(PARQUET_FILE)

    # Create date_str column if it doesn't exist
    if "date_str" not in parquet_df.columns:
        logger.info("Creating temporary date_str column for validation")
        parquet_df["date_str"] = parquet_df["crawl_date"].dt.strftime("%Y-%m-%d")

    parquet_counts = parquet_df.groupby("date_str").size().to_dict()
    total_parquet_records = len(parquet_df)

    # Compare counts
    logger.info(f"Total CSV records: {total_csv_records}")
    logger.info(f"Total Parquet records: {total_parquet_records}")

    count_match = total_csv_records == total_parquet_records
    logger.info(f"Total count match: {count_match}")

    # Check counts for each date
    date_mismatches = []
    for date, csv_count in date_record_counts.items():
        parquet_count = parquet_counts.get(date, 0)
        if csv_count != parquet_count:
            date_mismatches.append((date, csv_count, parquet_count))

    if date_mismatches:
        logger.warning(f"Found {len(date_mismatches)} dates with count mismatches:")
        for date, csv_count, parquet_count in date_mismatches:
            logger.warning(f"  {date}: CSV={csv_count}, Parquet={parquet_count}")
    else:
        logger.info("All date record counts match between CSV and Parquet!")

    return count_match, date_mismatches


# Schema validation function
def validate_schema_coverage():
    """Validate that all CSV columns are represented in the parquet schema."""
    logger.info("Validating schema coverage...")

    # Get all unique columns from CSVs
    csv_files = glob.glob(os.path.join(CSV_DIR, "*.csv"))
    all_csv_columns = set()

    for csv_file in tqdm(csv_files, desc="Examining CSV schemas", leave=False):
        # Just read the header row
        df = pd.read_csv(csv_file, nrows=0)
        all_csv_columns.update(df.columns)

    # Get parquet columns
    parquet_df = pd.read_parquet(PARQUET_FILE)
    parquet_columns = set(parquet_df.columns)

    # Expected transformations from original conversion
    expected_transformations = {
        "financialAssets": [
            "asset_exchanges",
            "asset_tickers",
            "asset_companies",
            "asset_shares",
            "asset_prices",
            "asset_currencies",
            "asset_exchange_rates",
        ],
        "industries": ["industry_codes"],
        "countryOfCitizenship": ["country_code"],
        "source": ["source_code"],
    }

    # Adjusted column expectations
    expected_columns = set()
    for col in all_csv_columns:
        if col in expected_transformations:
            expected_columns.update(expected_transformations[col])
        else:
            expected_columns.add(col)

    # Also expect date-related columns
    expected_columns.update(["crawl_date", "year", "month", "day"])

    # Don't require date_str column - it's derived and optional
    if "date_str" in parquet_columns:
        expected_columns.add("date_str")

    # Check if all expected columns are present
    missing_columns = expected_columns - parquet_columns
    extra_columns = parquet_columns - expected_columns

    if missing_columns:
        logger.warning(f"Missing expected columns in parquet: {missing_columns}")
    else:
        logger.info("All expected columns are present in parquet!")

    if extra_columns:
        logger.info(f"Extra columns in parquet (not in original CSV): {extra_columns}")

    return len(missing_columns) == 0, missing_columns


# Value validation function
def validate_sample_values(sample_size=10, billionaires_per_date=3):
    """
    Validate that sample values match between CSV and parquet.
    Performs deep validation of both scalar and complex fields.

    Args:
        sample_size: Number of dates to sample
        billionaires_per_date: Number of billionaires to test per date
    """
    logger.info(
        f"Validating sample values (dates: {sample_size}, billionaires per date: {billionaires_per_date})..."
    )

    # Get all CSV files
    csv_files = glob.glob(os.path.join(CSV_DIR, "*.csv"))

    # Load dictionaries
    dictionaries = load_dictionaries()

    # Load parquet data
    parquet_df = pd.read_parquet(PARQUET_FILE)

    # Create date_str column if it doesn't exist
    if "date_str" not in parquet_df.columns:
        logger.info("Creating temporary date_str column for validation")
        parquet_df["date_str"] = parquet_df["crawl_date"].dt.strftime("%Y-%m-%d")

    # Choose random dates to sample
    if len(csv_files) <= sample_size:
        sampled_files = csv_files
    else:
        sampled_files = random.sample(csv_files, sample_size)

    # Track validation results
    validation_results = []

    for csv_file in tqdm(sampled_files, desc="Validating samples"):
        date = os.path.basename(csv_file).split(".")[0]
        logger.info(f"Validating samples for date: {date}")

        # Read CSV file
        csv_df = pd.read_csv(csv_file)

        # Extract parquet data for this date
        parquet_date_df = parquet_df[parquet_df["date_str"] == date].copy()

        # Skip if either source has no records
        if len(csv_df) == 0 or len(parquet_date_df) == 0:
            logger.warning(f"No records found for date {date} in one of the sources")
            continue

        # Choose multiple random billionaires to compare
        billionaire_names = csv_df["personName"].tolist()
        # Limit to the number of billionaires available
        actual_billionaires_per_date = min(
            billionaires_per_date, len(billionaire_names)
        )

        if actual_billionaires_per_date < billionaires_per_date:
            logger.warning(
                f"Only {actual_billionaires_per_date} billionaires available for date {date}"
            )

        sampled_billionaires = random.sample(
            billionaire_names, actual_billionaires_per_date
        )

        logger.info(f"Testing {len(sampled_billionaires)} billionaires for date {date}")

        # Test each billionaire
        for billionaire_name in sampled_billionaires:
            # Extract records from both sources
            try:
                csv_record = (
                    csv_df[csv_df["personName"] == billionaire_name].iloc[0].to_dict()
                )
                parquet_record = parquet_date_df[
                    parquet_date_df["personName"] == billionaire_name
                ]

                if len(parquet_record) == 0:
                    logger.error(
                        f"Billionaire {billionaire_name} not found in parquet for date {date}"
                    )
                    validation_results.append(
                        {
                            "date": date,
                            "billionaire": billionaire_name,
                            "found_in_parquet": False,
                            "matching_fields": {},
                            "mismatching_fields": {
                                "all": "billionaire not found in parquet"
                            },
                        }
                    )
                    continue

                parquet_record = parquet_record.iloc[0].to_dict()

                # Compare basic fields
                matching_fields = {}
                mismatching_fields = {}

                for field in [
                    "finalWorth",
                    "estWorthPrev",
                    "privateAssetsWorth",
                    "archivedWorth",
                ]:
                    csv_value = csv_record.get(field)
                    parquet_value = parquet_record.get(field)

                    if pd.isna(csv_value) and pd.isna(parquet_value):
                        matching_fields[field] = "Both NA"
                    elif pd.isna(csv_value) or pd.isna(parquet_value):
                        mismatching_fields[field] = (
                            f"CSV: {csv_value}, Parquet: {parquet_value}"
                        )
                    elif (
                        abs(float(csv_value) - float(parquet_value)) < 0.001
                    ):  # Allow small floating point differences
                        matching_fields[field] = f"{csv_value}"
                    else:
                        mismatching_fields[field] = (
                            f"CSV: {csv_value}, Parquet: {parquet_value}"
                        )

                # Compare complex fields - financialAssets
                if "financialAssets" in csv_record:
                    csv_assets = parse_complex_field(csv_record["financialAssets"])

                    if isinstance(csv_assets, list) and len(csv_assets) > 0:
                        # Check if first asset's key properties match
                        try:
                            # Extract first asset info from CSV
                            first_csv_asset = csv_assets[0]

                            # Try to find matching asset in parquet
                            if (
                                "asset_tickers" in parquet_record
                                and len(parquet_record["asset_tickers"]) > 0
                            ):
                                ticker_match = (
                                    first_csv_asset.get("ticker")
                                    == parquet_record["asset_tickers"][0]
                                )
                                shares_match = (
                                    abs(
                                        float(
                                            first_csv_asset.get("numberOfShares", 0)
                                            or 0
                                        )
                                        - float(parquet_record["asset_shares"][0])
                                    )
                                    < 0.001
                                )

                                if ticker_match and shares_match:
                                    matching_fields["financialAssets_first_asset"] = (
                                        "Ticker and shares match"
                                    )
                                else:
                                    mismatching_fields[
                                        "financialAssets_first_asset"
                                    ] = f"Ticker match: {ticker_match}, Shares match: {shares_match}"
                            else:
                                mismatching_fields["financialAssets"] = (
                                    "Asset tickers not found in parquet record"
                                )
                        except (IndexError, KeyError, TypeError) as e:
                            mismatching_fields["financialAssets_error"] = str(e)
                    elif csv_assets == [] and (
                        "asset_tickers" not in parquet_record
                        or len(parquet_record.get("asset_tickers", [])) == 0
                    ):
                        matching_fields["financialAssets"] = "Both empty"
                    else:
                        mismatching_fields["financialAssets"] = "Structure mismatch"

                # Compare complex fields - industries
                if "industries" in csv_record:
                    csv_industries = parse_complex_field(csv_record["industries"])

                    if "industry_codes" in parquet_record:
                        # Try to decode industry codes
                        if dictionaries.get("industries_rev"):
                            decoded_industries = [
                                dictionaries["industries_rev"].get(
                                    code, f"Unknown_{code}"
                                )
                                for code in parquet_record["industry_codes"]
                            ]

                            # Check if industries match
                            industries_match = set(csv_industries) == set(
                                decoded_industries
                            )
                            if industries_match:
                                matching_fields["industries"] = f"{csv_industries}"
                            else:
                                mismatching_fields["industries"] = (
                                    f"CSV: {csv_industries}, Parquet decoded: {decoded_industries}"
                                )
                        else:
                            mismatching_fields["industries_decode"] = (
                                "Industries dictionary not found"
                            )
                    else:
                        mismatching_fields["industries"] = (
                            "Industry codes not found in parquet record"
                        )

                # Log results for this billionaire
                if mismatching_fields:
                    logger.warning(
                        f"Mismatches found for {billionaire_name} on {date}:"
                    )
                    for field, mismatch in mismatching_fields.items():
                        logger.warning(f"  {field}: {mismatch}")
                else:
                    logger.info(
                        f"All checked fields match for {billionaire_name} on {date}"
                    )

                # Save validation result
                validation_results.append(
                    {
                        "date": date,
                        "billionaire": billionaire_name,
                        "found_in_parquet": True,
                        "matching_fields": matching_fields,
                        "mismatching_fields": mismatching_fields,
                    }
                )
            except Exception as e:
                logger.error(f"Error validating {billionaire_name} on {date}: {str(e)}")
                validation_results.append(
                    {
                        "date": date,
                        "billionaire": billionaire_name,
                        "found_in_parquet": False,
                        "matching_fields": {},
                        "mismatching_fields": {"error": str(e)},
                    }
                )

    # Calculate overall validation success rate
    success_count = sum(
        1
        for result in validation_results
        if result["found_in_parquet"] and not result["mismatching_fields"]
    )
    total_count = len(validation_results)
    success_rate = success_count / total_count if total_count > 0 else 0

    logger.info(
        f"Value validation success rate: {success_rate:.2%} ({success_count}/{total_count})"
    )
    logger.info(
        f"Tested {len(sampled_files)} dates with up to {billionaires_per_date} billionaires each"
    )

    # Save validation results
    os.makedirs(VALIDATION_DIR, exist_ok=True)
    with open(os.path.join(VALIDATION_DIR, "sample_validation.json"), "w") as f:
        json.dump(validation_results, f, indent=2, default=str)

    return success_rate >= 0.9, validation_results


# Aggregate validation function
def validate_aggregates(sample_size=10):
    """
    Validate that aggregated values match between CSV and parquet data.

    Args:
        sample_size: Number of dates to sample for validation
    """
    logger.info(f"Validating aggregate calculations with {sample_size} samples...")

    # Function to process a file similar to your original process_file_lazy
    def process_file(file_path, is_csv=True, min_worth=0):
        if is_csv:
            # Process CSV
            df = pd.read_csv(file_path)
            filtered = df[
                (df["finalWorth"] >= min_worth)
                & (df["estWorthPrev"] >= min_worth)
                & (df["archivedWorth"] >= min_worth)
                & (df["privateAssetsWorth"] >= min_worth)
            ]
            result = {
                "N_Bi": len(filtered),
                "totF": filtered["finalWorth"].sum() / 1e3,
                "totB": filtered["estWorthPrev"].sum() / 1e3,
                "totA": filtered["archivedWorth"].sum() / 1e3,
                "totP": filtered["privateAssetsWorth"].sum() / 1e3,
            }
            return result
        else:
            # Process parquet
            date = os.path.basename(file_path).split(".")[0]
            df = pd.read_parquet(PARQUET_FILE)

            # Create date_str column if it doesn't exist
            if "date_str" not in df.columns:
                df["date_str"] = df["crawl_date"].dt.strftime("%Y-%m-%d")

            df = df[df["date_str"] == date]
            filtered = df[
                (df["finalWorth"] >= min_worth)
                & (df["estWorthPrev"] >= min_worth)
                & (df["archivedWorth"] >= min_worth)
                & (df["privateAssetsWorth"] >= min_worth)
            ]
            result = {
                "N_Bi": len(filtered),
                "totF": filtered["finalWorth"].sum() / 1e3,
                "totB": filtered["estWorthPrev"].sum() / 1e3,
                "totA": filtered["archivedWorth"].sum() / 1e3,
                "totP": filtered["privateAssetsWorth"].sum() / 1e3,
            }
            return result

    # Select sample dates to validate
    csv_files = glob.glob(os.path.join(CSV_DIR, "*.csv"))
    actual_sample_size = min(sample_size, len(csv_files))
    logger.info(
        f"Using {actual_sample_size} out of {len(csv_files)} available dates for aggregation validation"
    )
    sampled_files = random.sample(csv_files, actual_sample_size)

    # Process each sample file with both formats
    results = []
    for csv_file in tqdm(sampled_files, desc="Comparing aggregates"):
        date = os.path.basename(csv_file).split(".")[0]

        # Process as billionaires (worth >= 1B)
        csv_billion = process_file(csv_file, is_csv=True, min_worth=1000)
        parquet_billion = process_file(csv_file, is_csv=False, min_worth=1000)

        # Process as millionaires (worth >= 1M)
        csv_million = process_file(csv_file, is_csv=True, min_worth=1)
        parquet_million = process_file(csv_file, is_csv=False, min_worth=1)

        # Compare results
        billion_matches = {
            metric: abs(csv_billion[metric] - parquet_billion[metric]) < 0.001
            for metric in ["N_Bi", "totF", "totB", "totA", "totP"]
        }

        million_matches = {
            metric: abs(csv_million[metric] - parquet_million[metric]) < 0.001
            for metric in ["N_Bi", "totF", "totB", "totA", "totP"]
        }

        # Log results
        if all(billion_matches.values()) and all(million_matches.values()):
            logger.info(f"All aggregates match for {date}")
        else:
            logger.warning(f"Aggregate mismatches for {date}:")
            if not all(billion_matches.values()):
                logger.warning("  Billionaires mismatches:")
                for metric, matches in billion_matches.items():
                    if not matches:
                        logger.warning(
                            f"    {metric}: CSV={csv_billion[metric]}, Parquet={parquet_billion[metric]}"
                        )

            if not all(million_matches.values()):
                logger.warning("  Millionaires mismatches:")
                for metric, matches in million_matches.items():
                    if not matches:
                        logger.warning(
                            f"    {metric}: CSV={csv_million[metric]}, Parquet={parquet_million[metric]}"
                        )

        # Save result
        results.append(
            {
                "date": date,
                "billionaires": {
                    "csv": csv_billion,
                    "parquet": parquet_billion,
                    "all_match": all(billion_matches.values()),
                    "mismatches": {k: v for k, v in billion_matches.items() if not v},
                },
                "millionaires": {
                    "csv": csv_million,
                    "parquet": parquet_million,
                    "all_match": all(million_matches.values()),
                    "mismatches": {k: v for k, v in million_matches.items() if not v},
                },
            }
        )

    # Calculate overall success rate
    billionaire_success = sum(1 for r in results if r["billionaires"]["all_match"])
    millionaire_success = sum(1 for r in results if r["millionaires"]["all_match"])

    billionaire_rate = billionaire_success / sample_size
    millionaire_rate = millionaire_success / sample_size

    logger.info(
        f"Billionaire aggregate match rate: {billionaire_rate:.2%} ({billionaire_success}/{sample_size})"
    )
    logger.info(
        f"Millionaire aggregate match rate: {millionaire_rate:.2%} ({millionaire_success}/{sample_size})"
    )

    # Save results
    os.makedirs(VALIDATION_DIR, exist_ok=True)
    with open(os.path.join(VALIDATION_DIR, "aggregate_validation.json"), "w") as f:
        json.dump(results, f, indent=2, default=str)

    # Create comparison plot for visualization
    if results:
        plt.figure(figsize=(10, 6))
        dates = [r["date"] for r in results]
        csv_values = [r["billionaires"]["csv"]["totF"] for r in results]
        parquet_values = [r["billionaires"]["parquet"]["totF"] for r in results]

        plt.plot(dates, csv_values, "o-", label="CSV Total Final Worth")
        plt.plot(dates, parquet_values, "x--", label="Parquet Total Final Worth")
        plt.title("CSV vs Parquet: Billionaire Total Final Worth")
        plt.xlabel("Date")
        plt.ylabel("Billions of Dollars")
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()

        # Save plot
        os.makedirs(VALIDATION_DIR, exist_ok=True)
        plt.savefig(os.path.join(VALIDATION_DIR, "aggregate_comparison.png"))

    return billionaire_rate >= 0.9 and millionaire_rate >= 0.9, results


# Function to test reading CSV file for a specific billionaire
def find_billionaire_records(name, parquet_df=None):
    """
    Find and compare records for a specific billionaire in both formats.
    Useful for manual investigation of discrepancies.
    """
    logger.info(f"Searching for records of billionaire: {name}")

    # Load parquet data if not provided
    if parquet_df is None:
        parquet_df = pd.read_parquet(PARQUET_FILE)
        # Create date_str if it doesn't exist
        if "date_str" not in parquet_df.columns:
            parquet_df["date_str"] = parquet_df["crawl_date"].dt.strftime("%Y-%m-%d")

    # Find in parquet
    parquet_records = parquet_df[
        parquet_df["personName"].str.contains(name, case=False, na=False)
    ]
    logger.info(f"Found {len(parquet_records)} records in parquet")

    # Find in CSV
    csv_records = []
    csv_files = glob.glob(os.path.join(CSV_DIR, "*.csv"))
    for csv_file in tqdm(csv_files, desc="Searching CSVs", leave=False):
        date = os.path.basename(csv_file).split(".")[0]
        try:
            df = pd.read_csv(csv_file)
            matches = df[df["personName"].str.contains(name, case=False, na=False)]
            if len(matches) > 0:
                for _, row in matches.iterrows():
                    record = row.to_dict()
                    record["date"] = date
                    csv_records.append(record)
        except Exception as e:
            logger.error(f"Error reading {csv_file}: {e}")

    logger.info(f"Found {len(csv_records)} records in CSV files")

    # Compare dates of records
    csv_dates = sorted(set(r["date"] for r in csv_records))
    parquet_dates = sorted(set(parquet_records["date_str"]))

    date_match = set(csv_dates) == set(parquet_dates)
    logger.info(f"Date sets match: {date_match}")

    if not date_match:
        only_csv = set(csv_dates) - set(parquet_dates)
        only_parquet = set(parquet_dates) - set(csv_dates)
        if only_csv:
            logger.warning(f"Dates only in CSV: {only_csv}")
        if only_parquet:
            logger.warning(f"Dates only in Parquet: {only_parquet}")

    # Return the records for further analysis
    return {
        "csv_records": csv_records,
        "parquet_records": parquet_records,
        "date_match": date_match,
        "csv_dates": csv_dates,
        "parquet_dates": parquet_dates,
    }


# Main function to run all validations
def main():
    """Run all validation tests and report results."""
    logger.info("Starting comprehensive data validation")

    # Create validation directory
    os.makedirs(VALIDATION_DIR, exist_ok=True)

    # Set sample sizes for validation
    sample_value_size = 100  # Increased from default 10
    billionaires_per_date = 50  # Number of billionaires to test per date
    aggregate_sample_size = 100  # Increased from default 10

    # Run validations
    passed_count = 0
    total_tests = 4

    # 1. Record count validation
    count_passed, count_details = validate_record_counts()
    if count_passed:
        passed_count += 1

    # 2. Schema validation
    schema_passed, schema_details = validate_schema_coverage()
    if schema_passed:
        passed_count += 1

    # 3. Sample value validation with increased sample size and multiple billionaires per date
    values_passed, value_details = validate_sample_values(
        sample_size=sample_value_size, billionaires_per_date=billionaires_per_date
    )
    if values_passed:
        passed_count += 1

    # 4. Aggregate validation with custom sample size
    agg_passed, agg_details = validate_aggregates(sample_size=aggregate_sample_size)
    if agg_passed:
        passed_count += 1

    # Overall results
    logger.info("\n=== VALIDATION SUMMARY ===")
    logger.info(f"Tests passed: {passed_count}/{total_tests}")
    logger.info(f"Record count validation: {'PASSED' if count_passed else 'FAILED'}")
    logger.info(f"Schema validation: {'PASSED' if schema_passed else 'FAILED'}")
    logger.info(
        f"Sample value validation: {'PASSED' if values_passed else 'FAILED'} (using {sample_value_size} dates with up to {billionaires_per_date} billionaires each)"
    )
    logger.info(
        f"Aggregate validation: {'PASSED' if agg_passed else 'FAILED'} (using {aggregate_sample_size} samples)"
    )

    final_result = passed_count == total_tests
    logger.info(
        f"\nOverall validation result: {'PASSED' if final_result else 'FAILED'}"
    )
    logger.info("Check validation_results directory for detailed reports.")

    if final_result:
        logger.info("It should be safe to remove the original CSV files.")
    else:
        logger.warning("Please investigate the issues before removing original data.")

    # Save final report
    report = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tests_passed": passed_count,
        "total_tests": total_tests,
        "record_count_validation": {
            "passed": count_passed,
            "details": (
                [
                    {"date": d[0], "csv_count": d[1], "parquet_count": d[2]}
                    for d in count_details
                ]
                if count_details
                else []
            ),
        },
        "schema_validation": {
            "passed": schema_passed,
            "missing_columns": list(schema_details) if schema_details else [],
        },
        "sample_value_validation": {
            "passed": values_passed,
            "sample_count": len(value_details),
            "dates_sampled": sample_value_size,
            "billionaires_per_date": billionaires_per_date,
            "total_billionaires_sampled": len(value_details),
            "success_count": sum(
                1
                for r in value_details
                if r["found_in_parquet"] and not r["mismatching_fields"]
            ),
            "success_rate": (
                sum(
                    1
                    for r in value_details
                    if r["found_in_parquet"] and not r["mismatching_fields"]
                )
                / len(value_details)
                if value_details
                else 0
            ),
        },
        "aggregate_validation": {
            "passed": agg_passed,
            "sample_count": len(agg_details),
            "sample_size_requested": aggregate_sample_size,
            "billionaire_success_rate": (
                sum(1 for r in agg_details if r["billionaires"]["all_match"])
                / len(agg_details)
                if agg_details
                else 0
            ),
            "millionaire_success_rate": (
                sum(1 for r in agg_details if r["millionaires"]["all_match"])
                / len(agg_details)
                if agg_details
                else 0
            ),
        },
        "overall_passed": final_result,
    }

    with open(os.path.join(VALIDATION_DIR, "validation_summary.json"), "w") as f:
        json.dump(report, f, indent=2, default=str)

    return final_result


if __name__ == "__main__":
    main()

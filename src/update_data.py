#!/usr/bin/env python3
"""
RedFlagProfits Data Update Pipeline

Clean, focused script that fetches and processes billionaire data.
"""

import logging
from pathlib import Path

from data_backend import Config, ForbesClient, FredClient, DataProcessor, ParquetManager


def setup_logging():
    """Configure logging."""
    # Ensure log file directory exists
    log_path = Path(Config.LOG_FILE)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_path), logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


def main():
    """Main pipeline execution."""
    logger = setup_logging()
    logger.info("🚀 Starting RedFlagProfits data update pipeline")

    # Ensure directories exist using pathlib
    Config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    Config.DICT_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # Initialize components
        forbes_client = ForbesClient(logger)
        fred_client = FredClient(logger)
        processor = DataProcessor(logger)
        file_manager = ParquetManager(logger)

        # Step 1: Fetch Forbes data
        forbes_data, date_str = forbes_client.fetch_data()
        if forbes_data is None:
            logger.error("❌ Pipeline failed: No Forbes data")
            return False

        # Step 2: Fetch inflation data
        crawl_date = forbes_data["crawl_date"].iloc[0]
        cpi_value, pce_value = fred_client.get_inflation_data(crawl_date)

        # Step 3: Process data
        processed_data = processor.process_data(forbes_data)
        processed_data = processor.add_inflation_data(
            processed_data, cpi_value, pce_value
        )

        # Step 4: Save to parquet (handles duplicates automatically)
        success = file_manager.update_dataset(processed_data, date_str)
        if not success:
            return False

        # Step 5: Save dictionaries
        processor.save_dictionaries()

        logger.info("🎉 Pipeline completed successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

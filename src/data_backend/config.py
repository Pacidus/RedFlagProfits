"""Configuration and constants for the RedFlagProfits pipeline."""

import os
from dataclasses import dataclass


@dataclass
class Config:
    """Configuration settings."""

    # File paths
    DATA_DIR = "data"
    DICT_DIR = "data/dictionaries"
    PARQUET_FILE = "data/all_billionaires.parquet"
    LOG_FILE = "update.log"

    # API endpoints
    FORBES_API = "https://www.forbes.com/forbesapi/person/rtb/0/-estWorthPrev/true.json"
    FRED_API = "https://api.stlouisfed.org/fred/series/observations"

    # FRED series IDs
    CPI_SERIES = "CPIAUCNS"
    PCE_SERIES = "PCEPI"

    # Parquet settings
    COMPRESSION_LEVEL = 22
    DATA_PAGE_SIZE = 1048576
    DICT_PAGE_SIZE = 1048576

    # Data processing
    FORBES_COLUMNS = [
        "finalWorth",
        "estWorthPrev",
        "privateAssetsWorth",
        "archivedWorth",
        "personName",
        "gender",
        "birthDate",
        "countryOfCitizenship",
        "state",
        "city",
        "source",
        "industries",
        "financialAssets",
    ]

    GENDER_MAP = {"M": 0, "F": 1}
    INFLATION_BUFFER_DAYS = 90

    # Request headers
    HEADERS = {
        "authority": "www.forbes.com",
        "cache-control": "max-age=0",
        "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
    }

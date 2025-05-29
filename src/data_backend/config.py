"""Configuration and constants for the RedFlagProfits pipeline."""

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Config:
    """Configuration settings."""

    # File paths using pathlib
    DATA_DIR: Path = field(default_factory=lambda: Path("data"))
    DICT_DIR: Path = field(default_factory=lambda: Path("data/dictionaries"))
    PARQUET_FILE: Path = field(
        default_factory=lambda: Path("data/all_billionaires.parquet")
    )
    LOG_FILE: Path = field(default_factory=lambda: Path("update.log"))

    # API endpoints
    FORBES_API: str = (
        "https://www.forbes.com/forbesapi/person/rtb/0/-estWorthPrev/true.json"
    )
    FRED_API: str = "https://api.stlouisfed.org/fred/series/observations"

    # FRED series IDs
    CPI_SERIES: str = "CPIAUCNS"
    PCE_SERIES: str = "PCEPI"

    # Network settings
    REQUEST_TIMEOUT: int = 15
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 1

    # Parquet settings
    COMPRESSION_LEVEL: int = 22
    DATA_PAGE_SIZE: int = 1048576
    DICT_PAGE_SIZE: int = 1048576

    # Data processing
    FORBES_COLUMNS: list = field(
        default_factory=lambda: [
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
    )

    # Asset processing columns
    ASSET_COLUMNS: list = field(
        default_factory=lambda: [
            "exchanges",
            "tickers",
            "companies",
            "shares",
            "prices",
            "currencies",
            "exchange_rates",
        ]
    )

    # Dictionary names for encoding
    DICTIONARY_NAMES: list = field(
        default_factory=lambda: [
            "exchanges",
            "currencies",
            "industries",
            "companies",
            "countries",
            "sources",
        ]
    )

    # Constants
    GENDER_MAP: dict = field(default_factory=lambda: {"M": 0, "F": 1})
    INFLATION_BUFFER_DAYS: int = 90
    INVALID_CODE: int = -1

    # Asset field mappings
    ASSET_FIELD_MAPPINGS: list = field(
        default_factory=lambda: [
            ("shares", "numberOfShares", 0.0),
            ("prices", "sharePrice", 0.0),
            ("exchange_rates", "exchangeRate", 1.0),
        ]
    )

    # Request headers
    HEADERS: dict = field(
        default_factory=lambda: {
            "authority": "www.forbes.com",
            "cache-control": "max-age=0",
            "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
        }
    )


Config = Config()

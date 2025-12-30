"""Common utilities for the data backend."""

import time
import requests
from functools import wraps
from .config import Config


def retry_on_network_error(logger=None, operation_name="operation"):
    """Decorator for handling network retries with exponential backoff.

    If logger is None, will attempt to get logger from the instance (args[0].logger).
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Try to get logger from instance if not provided
            actual_logger = logger
            if actual_logger is None and args and hasattr(args[0], "logger"):
                actual_logger = args[0].logger

            for attempt in range(Config.MAX_RETRIES):
                try:
                    return func(*args, **kwargs)
                except (requests.ConnectionError, requests.Timeout) as e:
                    if actual_logger:
                        actual_logger.warning(
                            f"⚠️  {operation_name} network error "
                            f"(attempt {attempt + 1}/{Config.MAX_RETRIES}): {e}"
                        )
                    if attempt < Config.MAX_RETRIES - 1:
                        time.sleep(Config.RETRY_DELAY * (attempt + 1))
                        continue
                    if actual_logger:
                        actual_logger.error(
                            f"❌ {operation_name} failed after all retries"
                        )
                    return None
                except requests.HTTPError as e:
                    if actual_logger:
                        actual_logger.error(f"❌ {operation_name} HTTP error: {e}")
                    return None
                except (KeyError, ValueError) as e:
                    if actual_logger:
                        actual_logger.error(
                            f"❌ {operation_name} data parsing error: {e}"
                        )
                    return None
            return None

        return wrapper

    return decorator


def safe_numeric_conversion(value, default=0.0):
    """Safely convert value to float with fallback."""
    try:
        return float(value) if value else default
    except (ValueError, TypeError):
        return default


def is_invalid_value(value):
    """Check if value is invalid/empty."""
    import pandas as pd

    return (
        value is None
        or value == ""
        or (hasattr(value, "__len__") and len(str(value)) == 0)
        or (pd.isna(value) if isinstance(value, (int, float)) else False)
    )

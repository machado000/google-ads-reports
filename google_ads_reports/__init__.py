"""
Google Ads Driver - A Python ETL module for Google Ads API data extraction.

This package provides tools for extracting, transforming, and loading Google Ads data
using the Google Ads API with pandas DataFrame outputs.
"""
import logging
from typing import Optional

from .client import GAdsReport
from .exceptions import (
    APIError,
    AuthenticationError,
    ConfigurationError,
    DataProcessingError,
    GAdsReportError,
    ValidationError,
)
from .models import GAdsReportModel, create_custom_report
from .utils import (
    load_credentials,
    validate_customer_id,
    get_month_date_pairs,
    create_output_directory,
    format_report_filename,
)

# Main exports
__all__ = [
    "GAdsReport",
    # Models
    "GAdsReportModel",
    "create_custom_report",
    # Utils
    "load_credentials",
    "validate_customer_id",
    "get_month_date_pairs",
    "create_output_directory",
    "format_report_filename",
    # Exceptions
    "GAdsReportError",
    "AuthenticationError",
    "ValidationError",
    "APIError",
    "DataProcessingError",
    "ConfigurationError",
    # __init__ functions
    "setup_logging",
]


def setup_logging(level: int = logging.INFO,
                  format_string: Optional[str] = None) -> None:
    """
    Setup logging configuration.

    Args:
        level (int): Logging level (default: INFO)
        format_string (Optional[str]): Custom format string
    """
    if format_string is None:
        format_string = '%(levelname)s - %(message)s'

    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=[
            logging.StreamHandler(),
        ]
    )

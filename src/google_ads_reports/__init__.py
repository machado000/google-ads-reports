"""
Google Ads Driver - A Python ETL module for Google Ads API data extraction.

This package provides tools for extracting, transforming, and loading Google Ads data
using the Google Ads API with pandas DataFrame outputs.
"""
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
    create_output_directory,
    format_report_filename,
    load_credentials,
    setup_logging,
    validate_customer_id,
)

__version__ = "0.1.0"
__author__ = "Joao Brito"

# Main exports
__all__ = [
    "GAdsReport",
    "GAdsReportModel",
    "create_custom_report",
    "load_credentials",
    "setup_logging",
    "validate_customer_id",
    "create_output_directory",
    "format_report_filename",
    # Exceptions
    "GAdsReportError",
    "AuthenticationError",
    "ValidationError",
    "APIError",
    "DataProcessingError",
    "ConfigurationError",
]

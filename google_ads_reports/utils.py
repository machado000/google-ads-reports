"""
Utility functions for the Google Ads driver module.
"""
import logging
import calendar
import csv
import json
import os
import yaml

from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

from .exceptions import ConfigurationError, ValidationError


def load_credentials(config_path: Optional[str] = None) -> dict[str, Any]:
    """
    Load Google Ads API credentials from YAML file.

    Args:
        config_path (Optional[str]): Path to the credentials file.
                                   If None, tries default locations.

    Returns:
        dict[str, Any]: Loaded credentials configuration

    Raises:
        FileNotFoundError: If credentials file is not found
        yaml.YAMLError: If YAML parsing fails
    """
    default_paths = [
        os.path.join("secrets", "google-ads.yaml"),
        os.path.join(os.path.expanduser("~"), ".google-ads.yaml"),
        "google-ads.yaml"
    ]

    if config_path:
        paths_to_try = [config_path]
    else:
        paths_to_try = default_paths

    for path in paths_to_try:
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    credentials = yaml.safe_load(f)

                if not credentials:
                    raise ConfigurationError(f"Credentials file {path} is empty")

                if not isinstance(credentials, dict):
                    raise ConfigurationError(f"Credentials file {path} must contain a YAML dictionary")

                return credentials

            except yaml.YAMLError as e:
                logging.error(f"Error parsing YAML file {path}: {e}")
                raise ConfigurationError(
                    f"Invalid YAML format in credentials file {path}",
                    original_error=e
                ) from e
            except IOError as e:
                raise ConfigurationError(
                    f"Failed to read credentials file {path}",
                    original_error=e
                ) from e

    raise ConfigurationError(
        f"Could not find credentials file in any of these locations: {paths_to_try}"
    )


def validate_customer_id(customer_id: str) -> str:
    """
    Validate and format Google Ads customer ID.

    Args:
        customer_id (str): The customer ID to validate

    Returns:
        str: Formatted customer ID (without dashes)

    Raises:
        ValidationError: If customer ID format is invalid
    """
    if not customer_id:
        raise ValidationError("Customer ID cannot be empty")

    if not isinstance(customer_id, str):
        raise ValidationError("Customer ID must be a string")

    # Remove dashes and whitespace
    clean_id = customer_id.replace("-", "").replace(" ", "")

    # Check if it's numeric and has correct length (typically 10 digits)
    if not clean_id.isdigit():
        raise ValidationError(f"Customer ID must be numeric, got: {customer_id}")

    if len(clean_id) != 10:
        logging.warning(f"Customer ID length is {len(clean_id)}, expected 10: {customer_id}")

    return clean_id


def get_month_date_pairs(start_date: date, end_date: date) -> list[tuple[date, date]]:
    """
    Breaks a date range into monthly (start_date, end_date) pairs.

    Args:
        start_date (date): The start date of the range.
        end_date (date): The end date of the range.

    Returns:
        list[tuple[date, date]]: List of (start_date, end_date) tuples for each month in the range.
    """
    # Ensure input dates are date objects
    if isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime):
        end_date = end_date.date()

    if end_date < start_date:
        raise ValueError("ERROR - Invalid dates: 'end_date' precedes 'start_date'")

    # Initialize the list to store the month periods
    month_periods = []

    # Iterate through each month
    current_date = start_date
    while current_date <= end_date:
        # Get the first day of the current month
        month_start = max(current_date, date(current_date.year, current_date.month, 1))

        # Get the last day of the current month using calendar.monthrange
        last_day = calendar.monthrange(current_date.year, current_date.month)[1]
        month_end = min(end_date, date(current_date.year, current_date.month, last_day))

        month_periods.append((month_start, month_end))

        # Move to the first day of the next month
        if current_date.month == 12:
            current_date = date(current_date.year + 1, 1, 1)
        else:
            current_date = date(current_date.year, current_date.month + 1, 1)

    return month_periods


def create_output_directory(path: str) -> Path:
    """
    Create output directory if it doesn't exist.

    Args:
        path (str): Directory path to create

    Returns:
        Path: Path object for the created directory
    """
    output_path = Path(path)
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path


def format_report_filename(report_name: str, customer_id: str,
                           start_date: str, end_date: str,
                           file_extension: str = "csv") -> str:
    """
    Generate a standardized filename for report exports.

    Args:
        report_name (str): Name of the report
        customer_id (str): Google Ads customer ID
        start_date (str): Report start date
        end_date (str): Report end date
        file_extension (str): File extension (default: csv)

    Returns:
        str: Formatted filename
    """
    # Clean the inputs
    safe_report_name = report_name.replace(" ", "_").lower()
    safe_customer_id = validate_customer_id(customer_id)

    return f"{safe_report_name}_{safe_customer_id}_{start_date}_{end_date}.{file_extension}"


def save_report_to_csv(data: list[dict[str, Any]], filepath: str) -> str:
    """
    Save report data to CSV file.

    Parameters:
    - data: List of dictionaries containing report data
    - filepath: Path where to save the CSV file

    Returns:
    - str: Full path of the saved CSV file

    Raises:
    - ConfigurationError: If CSV writing fails
    """
    try:
        # Add .csv extension if not present
        if not filepath.endswith('.csv'):
            filepath += '.csv'

        if not data:
            logging.warning("No data to save to CSV")
            # Create empty CSV file
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                csvfile.write('')
            return filepath

        # Get all unique keys from all dictionaries to use as fieldnames
        fieldnames_set: set[str] = set()
        for row in data:
            fieldnames_set.update(row.keys())
        fieldnames = sorted(list(fieldnames_set))

        # Write to CSV
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        logging.debug(f"Successfully saved {len(data)} rows to {filepath}")
        return filepath

    except Exception as e:
        raise ConfigurationError("Failed to save CSV file:", original_error=e) from e


def save_report_to_json(data: list[dict[str, Any]], filepath: str, indent: int = 2) -> str:
    """
    Save report data to JSON file.

    Parameters:
    - data: List of dictionaries containing report data
    - filepath: Path where to save the JSON file
    - indent: JSON indentation level (default: 2)

    Returns:
    - str: Full path of the saved JSON file

    Raises:
    - ConfigurationError: If JSON writing fails
    """
    try:
        # Add .json extension if not present
        if not filepath.endswith('.json'):
            filepath += '.json'

        # Write to JSON
        with open(filepath, 'w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, indent=indent, ensure_ascii=False, default=str)

        logging.debug(f"Successfully saved {len(data)} rows to {filepath}")
        return filepath

    except Exception as e:
        raise ConfigurationError("Failed to save JSON file:", original_error=e) from e


# ============================================================================
# List of Dictionaries Utility Functions (replacing DataFrame operations)
# ============================================================================

def get_records_info(records: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Get essential information about the records dataset.

    Args:
        records: List of dictionary records

    Returns:
        dict[str, Any]: Dataset information with:
            - shape: tuple of (rows, columns)
            - columns: list of column names
    """
    if not records:
        return {"shape": (0, 0), "columns": []}

    # Get all unique column names
    all_columns: set[str] = set()
    for record in records:
        all_columns.update(record.keys())

    columns = sorted(list(all_columns))

    return {
        "shape": (len(records), len(columns)),
        "columns": columns
    }

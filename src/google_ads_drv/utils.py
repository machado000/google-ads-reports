"""
Utility functions for the Google Ads driver module.
"""
import os
import logging
import yaml
from typing import Dict, Any, Optional
from pathlib import Path


def load_credentials(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load Google Ads API credentials from YAML file.

    Args:
        config_path (Optional[str]): Path to the credentials file.
                                   If None, tries default locations.

    Returns:
        Dict[str, Any]: Loaded credentials configuration

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
                    return yaml.safe_load(f)
            except yaml.YAMLError as e:
                logging.error(f"Error parsing YAML file {path}: {e}")
                raise

    raise FileNotFoundError(f"Could not find credentials file in any of these locations: {paths_to_try}")


def setup_logging(level: int = logging.INFO,
                  format_string: Optional[str] = None) -> None:
    """
    Setup logging configuration.

    Args:
        level (int): Logging level (default: INFO)
        format_string (Optional[str]): Custom format string
    """
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=[
            logging.StreamHandler(),
        ]
    )


def validate_customer_id(customer_id: str) -> str:
    """
    Validate and format Google Ads customer ID.

    Args:
        customer_id (str): The customer ID to validate

    Returns:
        str: Formatted customer ID (without dashes)

    Raises:
        ValueError: If customer ID format is invalid
    """
    # Remove dashes and whitespace
    clean_id = customer_id.replace("-", "").replace(" ", "")

    # Check if it's numeric and has correct length (typically 10 digits)
    if not clean_id.isdigit():
        raise ValueError(f"Customer ID must be numeric, got: {customer_id}")

    if len(clean_id) != 10:
        logging.warning(f"Customer ID length is {len(clean_id)}, expected 10: {customer_id}")

    return clean_id


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

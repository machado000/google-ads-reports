"""
Google Ads API client module.

This module contains the main GAdsReport class for interacting with the Google Ads API.
"""
import logging
import socket
import tempfile
import os

from datetime import date, datetime
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict
from typing import Any
from .exceptions import AuthenticationError, DataProcessingError, ValidationError
from .retry import retry_on_api_error

# Python 3.12+ type aliases for better readability
type Record = dict[str, Any]
type RecordList = list[Record]
type ReportModel = dict[str, Any]
type APIResponse = dict[str, Any]

# Set timeout for all http connections
TIMEOUT_IN_SEC = 60 * 3  # seconds timeout limit
socket.setdefaulttimeout(TIMEOUT_IN_SEC)


class GAdsReport:
    """
    GAdsReport class for interacting with the Google Ads API v21.

    This class enables extraction of Google Ads data and transformation into optimized
    list of dictionaries ready for database storage. It provides comprehensive data
    processing, configurable missing value handling, character encoding cleanup,
    and flexible column naming conventions (snake_case or camelCase).

    Optimized for serverless environments by removing heavy dependencies like pandas
    and using pure Python data structures for better cold start performance.

    Parameters:
        client_secret (dict[str, Any] | None): Google Ads API authentication configuration

    Methods:
        get_gads_report: Main method to retrieve and process Google Ads report data
        get_default_report: Alias for get_gads_report (backward compatibility)

    Private Methods:
        _build_gads_query: Constructs GAQL queries for the Google Ads API
        _get_google_ads_response: Executes API requests with retry logic and pagination
        _convert_response_to_records: Converts protobuf responses to list of dictionaries
        _handle_missing_values: Configurable None handling for different value types
        _clean_text_encoding: Cleans text values for database compatibility
        _transform_column_names: Configurable column naming (snake_case or camelCase)

    Raises:
        AuthenticationError: Invalid credentials or authentication failure
        ValidationError: Invalid input parameters or configuration
        DataProcessingError: API response processing failures
    """

    def __init__(self, client_secret: ReportModel | None = None):
        """
        Initializes the GAdsReport instance.

        Parameters:
        - client_secret (dict): The YAML configuration for authentication.

        Raises:
        - AuthenticationError: If credentials are invalid or authentication fails
        - ValidationError: If client_secret format is invalid
        """

        if client_secret is not None:
            if not isinstance(client_secret, dict):
                raise ValidationError("client_secret must be a dictionary if provided")

            if not client_secret:
                raise ValidationError("client_secret cannot be empty if provided")

            try:
                # Initialize the Google Ads API client from dict
                self.client = GoogleAdsClient.load_from_dict(client_secret, version="v21")
                logging.info("Google Ads client authenticated from provided credentials")

            except Exception as e:
                logging.error(f"Authentication failed: {e}", exc_info=True)
                raise AuthenticationError(
                    "Failed to authenticate with Google Ads API using client_secret",
                    original_error=e
                ) from e
        else:
            try:
                # Initialize the Google Ads API client from environment
                load_dotenv()

                json_key = os.getenv("GOOGLE_ADS_JSON_KEY")
                if json_key:
                    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as tmp:
                        tmp.write(json_key)
                        tmp_path = tmp.name
                    os.environ["GOOGLE_ADS_JSON_KEY_FILE_PATH"] = tmp_path

                self.client = GoogleAdsClient.load_from_env(version="v21")
                logging.info("Google Ads client loaded from environment variables")

            except Exception as e:
                logging.error(f"Authentication failed (env): {e}", exc_info=True)
                raise AuthenticationError(
                    "Failed to authenticate with Google Ads API from environment",
                    original_error=e
                ) from e

        # Lazy initialization of service - only create when needed
        self._service = None

    @property
    def service(self) -> Any:
        """Lazy initialization of Google Ads service for better serverless performance."""
        if self._service is None:
            try:
                self._service = self.client.get_service("GoogleAdsService", version="v21")
            except Exception as e:
                logging.error(f"Failed to create Google Ads service: {e}", exc_info=True)
                raise AuthenticationError(
                    "Failed to create Google Ads API service",
                    original_error=e
                ) from e
        return self._service

    def get_gads_report(self, customer_id: str, report_model: ReportModel,
                        start_date: date, end_date: date,
                        filter_zero_impressions: bool = True,
                        column_naming: str = "snake_case") -> RecordList:
        """
        Retrieves and processes Google Ads report data for database insertion.

        This method executes a Google Ads API query, processes the response through a
        comprehensive data pipeline including missing value handling and character
        encoding cleanup to produce database-ready records.

        Parameters:
            customer_id (str): Google Ads customer ID
            report_model (ReportModel): Report configuration with 'select', 'from',
                optional 'where', 'order_by', and 'report_name' keys
            start_date (date): Report start date (inclusive)
            end_date (date): Report end date (inclusive)
            filter_zero_impressions (bool): Remove rows with zero impressions.
                Handles multiple zero formats: 0, "0", 0.0, "0.0", None
            column_naming (str): Column naming convention. Options:
                - "snake_case": campaign.name → campaign_name (default)
                - "camelCase": campaign.name → campaignName

        Returns:
            RecordList: List of records with:
                - Database-compatible column names in chosen format
                - Cleaned text encoding (ASCII-safe, max 255 chars)
                - Preserved None values for database NULL compatibility

        Raises:
            ValidationError: Invalid parameters or report model
            AuthenticationError: API authentication failure
            DataProcessingError: Response processing failure
        """

        response = self._get_google_ads_response(customer_id, report_model, start_date, end_date)

        result_records = self._convert_response_to_records(response, report_model)

        if result_records:
            # Filter out rows with zero impressions (configurable behavior)
            # Only apply filtering if the data contains impression metrics
            if filter_zero_impressions and result_records and "metrics.impressions" in result_records[0]:
                original_count = len(result_records)
                result_records = [
                    record for record in result_records
                    if not self._is_zero_impression_record(record)
                ]
                removed_rows = original_count - len(result_records)
                if removed_rows > 0:
                    logging.info(f"Filtered out {removed_rows} rows with zero impressions.")

            elif filter_zero_impressions:
                logging.info("Skipping zero impressions filter: no 'metrics.impressions' column found in data.")

            # Handle missing values for database compatibility
            # result_records = self._handle_missing_values(result_records, fill_object_values="")

            # Clean text encoding for database compatibility
            result_records = self._clean_text_encoding(result_records)

            # Transform column names according to specified convention
            result_records = self._transform_column_names(result_records, naming_convention=column_naming)

        return result_records

    def _build_gads_query(self, report_model: ReportModel, start_date: date, end_date: date) -> str:
        """
        Creates a query string for the Google Ads API.

        Parameters:
        - report_model (ReportModel): The report model specifying dimensions, metrics, etc.
        - start_date (date): Start date for the report.
        - end_date (date): End date for the report.

        Returns:
        - str: The constructed query string.
        """
        # Convert datetime objs to strings
        start_date_iso = start_date.isoformat() if isinstance(start_date, (date, datetime)) else start_date
        end_date_iso = end_date.isoformat() if isinstance(end_date, (date, datetime)) else end_date

        # Initialize the query string with the SELECT and FROM clauses and append segments.date
        query_str = f"SELECT {', '.join(report_model['select'])} FROM {report_model['from']}"
        query_str += f" WHERE segments.date BETWEEN '{start_date_iso}' AND '{end_date_iso}'"

        # Add the WHERE clause if it exists in the query_dict
        if "where" in report_model:
            query_str += f" AND {report_model['where']}"

        # Add the ORDER BY clause
        if "order_by" in report_model:
            query_str += f" ORDER BY segments.date ASC, {report_model['order_by']} DESC"
        else:
            query_str += " ORDER BY segments.date ASC"

        return query_str

    @retry_on_api_error(max_attempts=3, base_delay=1.0)
    def _get_google_ads_response(self, customer_id: str, report_model: ReportModel,
                                 start_date: date, end_date: date) -> APIResponse:
        """
        Retrieves GAds report data using GoogleAdsClient().get_service().search() .

        Parameters:
        - customer_id (str): The customer ID for Google Ads.
        - report_model (ReportModel): The report model specifying dimensions, metrics, etc.
        - start_date (date): Start date for the report.
        - end_date (date): End date for the report.

        Returns:
        - APIResponse: GAds report data dict containing keys `results`, `totalResultsCount`, and `fieldMask`.

        Raises:
        - ValidationError: If input parameters are invalid
        - APIError: If Google Ads API request fails
        """
        # Validate inputs
        if not customer_id or not isinstance(customer_id, str):
            raise ValidationError("customer_id must be a non-empty string")

        if not isinstance(report_model, dict) or 'report_name' not in report_model:
            raise ValidationError("report_model must be a dict with 'report_name' key")

        # Display request parameters
        print("[ Request parameters ]\n"
              f"Resource: {type(self.service).__name__}\n"
              f"Customer_id: {customer_id}\n"
              f"Report_model: {report_model['report_name']}\n"
              f"Date range: from {start_date.isoformat()} to {end_date.isoformat()}\n"
              )

        try:
            query_str = self._build_gads_query(report_model, start_date, end_date)
        except Exception as e:
            raise ValidationError(
                "Failed to build query string",
                original_error=e,
                report_model=report_model.get('report_name', 'unknown')
            ) from e

        search_request = self.client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = customer_id  # type: ignore
        search_request.query = query_str  # type: ignore
        search_request.search_settings.return_total_results_count = True  # type: ignore
        # search_request.page_size = 100 # Deprecated in API v17, default as 10_000
        logging.debug(search_request)  # DEBUG only

        full_response_dict: APIResponse = {
            "results": [],
            "totalResultsCount": 0,
            "fieldMask": "",
        }

        # Execute the query and retrieve the results
        # Note: The retry decorator will handle GoogleAdsException retries
        # Execute the query to fetch the first page of data
        logging.info("Executing search request...")
        response = self.service.search(search_request)

        # Check if response has headers and results
        if hasattr(response, "field_mask") and response.total_results_count > 0:
            while True:
                try:
                    response_dict = MessageToDict(response._pb)
                    page_results = response_dict.get("results", [])
                    # Ensure page_results is a list
                    if not isinstance(page_results, list):
                        page_results = [page_results]
                    full_response_dict["results"].extend(page_results)

                    logging.info(
                        f"Request returned {len(full_response_dict["results"])}/{response.total_results_count} rows")

                    if response.next_page_token == "":
                        logging.debug("Response has no next_page_token")
                        break
                    else:
                        logging.debug(f"Executing search request with next_page_token: '{response.next_page_token}'")
                        search_request.page_token = response.next_page_token  # type: ignore
                        response = self.service.search(search_request)

                except Exception as e:
                    raise DataProcessingError(
                        "Failed to process API response pagination",
                        original_error=e,
                        customer_id=customer_id
                    ) from e

            full_response_dict["totalResultsCount"] = response.total_results_count
            full_response_dict["fieldMask"] = response_dict.get("fieldMask", "")

            logging.info(f"Finished fetching full Response with {len(full_response_dict['results'])} rows")

        else:
            logging.info("Response has no 'results' with requested parameters")

        return full_response_dict

    def _convert_response_to_records(self, response: APIResponse,
                                     report_model: ReportModel) -> RecordList:
        """
        Converts the Google Ads API protobuf response to list of dictionaries.

        Parameters:
        - response: The Google Ads API response in protobuf dict format.
        - report_model (ReportModel): The custom report model specifying dimensions.

        Returns:
        - RecordList: List of records containing GAds report data.

        Raises:
        - DataProcessingError: If conversion fails
        """
        try:
            if not response or "results" not in response:
                raise DataProcessingError("Response is empty or missing 'results' key")

            if not response["results"]:
                logging.info("No results returned, creating empty list")
                return []

            # Flatten nested dictionaries from the response
            records = []
            for result in response["results"]:
                flattened_record = self._flatten_dict(result)
                # Remove resource name fields
                cleaned_record = {
                    k: v for k, v in flattened_record.items()
                    if not k.endswith(".resourceName")
                }
                records.append(cleaned_record)

            return records

        except Exception as e:
            raise DataProcessingError(
                "Failed to convert API response to records",
                original_error=e,
                report_name=report_model.get('report_name', 'unknown')
            ) from e

    def _flatten_dict(self, nested_dict: Record, parent_key: str = "", sep: str = ".") -> Record:
        """
        Flattens nested dictionary structure.

        Parameters:
        - nested_dict: The nested dictionary to flatten
        - parent_key: The parent key for recursion
        - sep: Separator for nested keys

        Returns:
        - Record: Flattened dictionary
        """
        items: list[tuple[str, Any]] = []
        for k, v in nested_dict.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def _is_zero_impression_record(self, record: Record) -> bool:
        """
        Checks if a record has zero impressions.

        Parameters:
        - record: The record to check

        Returns:
        - bool: True if record has zero impressions
        """
        impression_value = record.get("metrics.impressions")

        if impression_value is None:
            return True

        # Fast path for common cases
        if impression_value == 0 or impression_value == "0":
            return True

        # Handle various zero representations
        try:
            return float(impression_value) == 0.0
        except (ValueError, TypeError):
            # If can't convert to float, check string representations
            str_value = str(impression_value).strip().lower()
            return str_value in {"0.0", "", "none", "null"}

    def _handle_missing_values(self, records: RecordList,
                               fill_object_values: str = "") -> RecordList:
        """
        Handles missing values appropriately for database compatibility.

        Parameters:
        - records: List of records to process
        - fill_object_values: Value to fill None in text fields (empty string by default)

        Returns:
        - RecordList: Processed records
        """
        if not records:
            return records

        processed_records = []
        for record in records:
            processed_record = {}
            for key, value in record.items():
                # Handle None values for text fields
                if value is None and fill_object_values != "":
                    processed_record[key] = fill_object_values
                else:
                    processed_record[key] = value
            processed_records.append(processed_record)

        return processed_records

    def _clean_text_encoding(self, records: RecordList) -> RecordList:
        """
        Cleans text values for character encoding issues.

        Parameters:
        - records: List of records to process

        Returns:
        - RecordList: Records with cleaned text values
        """
        if not records:
            return records

        try:
            cleaned_records = []
            for record in records:
                cleaned_record = {}
                for key, value in record.items():
                    if isinstance(value, str):
                        # Optimized text cleaning for serverless environments
                        # Remove non-ASCII characters, null bytes, and normalize whitespace
                        cleaned_value = (
                            value.encode('ascii', 'ignore')
                            .decode('ascii')
                            .replace('\x00', '')
                            .replace('\r', ' ')
                            .replace('\n', ' ')
                            .strip()[:255]  # Truncate to DB field limit
                        )
                        cleaned_record[key] = cleaned_value
                    else:
                        cleaned_record[key] = value
                cleaned_records.append(cleaned_record)
            return cleaned_records

        except Exception as e:
            logging.warning(f"Character encoding cleanup failed: {e}")
            return records

    def _transform_column_names(self, records: RecordList,
                                naming_convention: str = "snake_case") -> RecordList:
        """
        Transforms column names according to the specified naming convention.

        Parameters:
            records: List of records with original column names
            naming_convention (str):
                - "snake_case": campaign.name → campaign_name (default)
                - "camelCase": campaign.name → campaignName
        Returns:
            RecordList: Records with transformed column names
        """
        # Validate column naming parameter
        if naming_convention.lower() not in ["snake_case", "camelcase"]:
            naming_convention = "snake_case"
            logging.warning(f"Invalid column_naming '{naming_convention}'. Using 'snake_case' as default")

        if not records:
            return records

        try:
            transformed_records = []
            for record in records:
                transformed_record = {}
                for col, value in record.items():
                    if naming_convention.lower() == "snake_case":
                        # Remove prefixes and convert to snake_case
                        new_col = (col.replace("segments.", "")
                                   .replace("adGroupCriterion.", "")
                                   .replace("metrics.", "")
                                   .replace(".", "_")
                                   .lower())

                    elif naming_convention.lower() == "camelcase":
                        # Remove prefixes and convert to camelCase
                        clean_col = (col.replace("segments.", "")
                                     .replace("adGroupCriterion.", "")
                                     .replace("metrics.", ""))

                        # Convert to camelCase
                        parts = clean_col.split(".")
                        new_col = parts[0] + "".join(part.capitalize() for part in parts[1:])

                    transformed_record[new_col] = value
                transformed_records.append(transformed_record)

            return transformed_records

        except Exception as e:
            logging.warning(f"Column naming transformation failed: {e}")
            return records

"""
Google Ads API client module.

This module contains the main GAdsReport class for interacting with the Google Ads API.
"""
import logging
import socket
from datetime import date, datetime
from typing import Any, Dict

import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict

from .exceptions import AuthenticationError, DataProcessingError, ValidationError
from .retry import retry_on_api_error

# Set timeout for all http connections
TIMEOUT_IN_SEC = 60 * 3  # seconds timeout limit
socket.setdefaulttimeout(TIMEOUT_IN_SEC)


class GAdsReport:
    """
    GAdsReport class is designed to interact with the Google Ads API,
    enabling the extraction of data and transformation into a Pandas DataFrame.

    Parameters:
    - client_secret (dict): The YAML configuration for authentication.

    Methods:
    - __init__(self, client_secret): Initializes the GAdsReport instance.
    - _build_gads_query(self, report_model, start_date, end_date): Creates a query string for the Google Ads API.
    - _convert_response_to_df(self, response, report_model): Converts the API response to a Pandas DataFrame.
    - get_gads_report(self, customer_id, report_model, start_date, end_date): Retrieves GAds report data.
    """

    def __init__(self, client_secret: Dict[str, Any]):
        """
        Initializes the GAdsReport instance.

        Parameters:
        - client_secret (dict): The YAML configuration for authentication.

        Raises:
        - AuthenticationError: If credentials are invalid or authentication fails
        - ValidationError: If client_secret format is invalid
        """
        if not isinstance(client_secret, dict):
            raise ValidationError("client_secret must be a dictionary")

        if not client_secret:
            raise ValidationError("client_secret cannot be empty")

        try:
            # Initialize the Google Ads API client
            self.client = GoogleAdsClient.load_from_dict(client_secret, version="v20")

            logging.info("Google YAML credentials are valid!")
            logging.info("Successful client authentication using Google Ads API (GAds)")

        except Exception as e:
            logging.error(f"Authentication failed: {e}", exc_info=True)
            raise AuthenticationError(
                "Failed to authenticate with Google Ads API",
                original_error=e
            ) from e

        try:
            # Create a Google Ads API service client
            self.service = self.client.get_service("GoogleAdsService", version="v20")
        except Exception as e:
            logging.error(f"Failed to create Google Ads service: {e}", exc_info=True)
            raise AuthenticationError(
                "Failed to create Google Ads API service",
                original_error=e
            ) from e

    def _build_gads_query(self, report_model: Dict[str, Any], start_date: date, end_date: date) -> str:
        """
        Creates a query string for the Google Ads API.

        Parameters:
        - report_model (dict): The report model specifying dimensions, metrics, etc.
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
    def _get_google_ads_response(self, customer_id: str, report_model: Dict[str, Any],
                                 start_date: date, end_date: date) -> Dict[str, Any]:
        """
        Retrieves GAds report data using GoogleAdsClient().get_service().search() .

        Parameters:
        - customer_id (str): The customer ID for Google Ads.
        - report_model (dict): The report model specifying dimensions, metrics, etc.
        - start_date (date): Start date for the report.
        - end_date (date): End date for the report.

        Returns:
        - dict: GAds report data dict containing keys `results`, `totalResultsCount`, and `fieldMask`.

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
        print("\n[ Request parameters ]",
              f"Resource: {format(type(self.service).__name__)}",
              f"Customer_id: {customer_id}",
              f"Report_model: {report_model['report_name']}",
              f"Date range: from {start_date.isoformat()} to {end_date.isoformat()}",
              "",
              sep="\n")

        try:
            query_str = self._build_gads_query(report_model, start_date, end_date)
        except Exception as e:
            raise ValidationError(
                "Failed to build query string",
                original_error=e,
                report_model=report_model.get('report_name', 'unknown')
            ) from e
        # logging.info(query_str:)  # DEBUG

        search_request = self.client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = customer_id
        search_request.query = query_str
        search_request.search_settings.return_total_results_count = True
        # search_request.page_size = 100 # Deprecated in API v17, default as 10_000
        # logging.info(search_request:) # DEBUG only

        full_response_dict = {
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
                    full_response_dict["results"].extend(page_results)

                    logging.info(f"Request returned {len(page_results)}/{response.total_results_count} rows")

                    if response.next_page_token == "":
                        logging.info("Response has no next_page_token")
                        break
                    else:
                        logging.info(f"Executing search request with next_page_token: {response.next_page_token=}")
                        search_request.page_token = response.next_page_token
                        response = self.service.search(search_request)

                except Exception as e:
                    raise DataProcessingError(
                        "Failed to process API response pagination",
                        original_error=e,
                        customer_id=customer_id
                    ) from e

            full_response_dict["totalResultsCount"] = response.total_results_count
            full_response_dict["fieldMask"] = response_dict.get("fieldMask", "")

            logging.info(f"Finished fetching full report with {len(full_response_dict['results'])} rows")

        else:
            logging.info("Report has no 'results' with requested parameters")

        return full_response_dict

    def _convert_response_to_df(self, response: Dict[str, Any], report_model: Dict[str, Any]) -> pd.DataFrame:
        """
        Converts the Google Ads API protobuf response 'MessageToDict(response._pb)' to dataFrame.

        Parameters:
        - response: The Google Ads API response in protobuf dict format.
        - report_model (dict): The custom report model specifying dimensions to guide dataframe schema.

        Returns:
        - DataFrame: Pandas DataFrame containing GAds report data.

        Raises:
        - DataProcessingError: If DataFrame conversion fails
        """
        try:
            if not response or "results" not in response:
                raise DataProcessingError("Response is empty or missing 'results' key")

            if not response["results"]:
                logging.info("No results returned, creating empty DataFrame")
                return pd.DataFrame()

            # Create a DataFrame from the response data
            result_df = pd.json_normalize(response["results"])

            # Drop resource name columns
            columns_to_drop = [col for col in result_df.columns if ".resourceName" in col]
            if columns_to_drop:
                result_df = result_df.drop(columns=columns_to_drop)

            return result_df

        except Exception as e:
            raise DataProcessingError(
                "Failed to convert API response to DataFrame",
                original_error=e,
                report_name=report_model.get('report_name', 'unknown')
            ) from e

    def _fix_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimizes data types for database storage.
        """
        try:
            # 1. Fix date columns (these come as strings from API)
            date_columns = [col for col in df.columns if 'date' in col]
            for col in date_columns:
                if col in df.columns and df[col].dtype == 'object':
                    df[col] = pd.to_datetime(df[col], errors='raise')  # Fail fast if dates are invalid
                    logging.info(f"Converted {col} to datetime")

            # 2. Dynamically find and convert metric columns that come as 'object' but should be numeric
            metrics_to_convert = []

            # Find all columns that contain 'metrics' and are currently 'object' type
            for col in df.columns:
                if 'metrics' in col and df[col].dtype == 'object':
                    metrics_to_convert.append(col)

            for col in metrics_to_convert:
                try:
                    # First convert to numeric to see what we get
                    numeric_series = pd.to_numeric(df[col], errors='raise')

                    # Determine if it should be int or float based on the data
                    if self._should_be_integer(numeric_series):
                        df[col] = numeric_series.astype('int64')
                        logging.info(f"Converted {col} from object to int64")
                    else:
                        df[col] = numeric_series.astype('float64')
                        logging.info(f"Converted {col} from object to float64")

                except ValueError as e:
                    logging.warning(f"Could not convert {col} to numeric: {e}")
                    # Keep as object type if conversion fails

            return df

        except Exception as e:
            logging.error(f"Data type optimization failed: {e}")
            return df

    def _should_be_integer(self, numeric_series: pd.Series) -> bool:
        """
        Determines if a numeric series should be stored as integer or float.

        Parameters:
        - numeric_series: The pandas Series with numeric data

        Returns:
        - bool: True if should be integer, False if should be float
        """
        # Remove NaN values for analysis
        clean_series = numeric_series.dropna()

        if len(clean_series) == 0:
            return False  # Default to float if no data

        # If all values are whole numbers, use integer
        if (clean_series % 1 == 0).all():
            return True

        return False  # Has decimals, should be float

    def _handle_missing_values(self, df: pd.DataFrame,
                               fill_numeric_values: str = None,
                               fill_datetime_values: str = None,
                               fill_object_values: str = "") -> pd.DataFrame:
        """
        Handles missing values appropriately based on column types.

        Parameters:
        - fill_numeric_values: Value to fill NaN in numeric columns (empty = preserve NaN)
        - fill_datetime_values: Value to fill NaT in datetime columns (empty = preserve NaT)
        - fill_object_values: Value to fill NaN in object/text columns (empty string by default)
        """

        if not fill_datetime_values and not fill_numeric_values and not fill_object_values:
            logging.info("No fill values provided, preserving NaN/NaT for numeric and datetime columns")
            return df

        try:
            for col in df.columns:
                # Case 1: Numeric columns (int, float)
                if pd.api.types.is_numeric_dtype(df[col]) and fill_numeric_values != "":
                    try:
                        # Attempt to convert to numeric value
                        fill_val = float(fill_numeric_values)
                        df[col] = df[col].fillna(fill_val)
                    except (ValueError, TypeError):
                        pass  # Keep NaN if conversion fails

                # Case 2: Datetime columns
                elif pd.api.types.is_datetime64_any_dtype(df[col]) and fill_datetime_values != "":
                    try:
                        # Attempt to convert to datetime
                        fill_datetime = pd.to_datetime(fill_datetime_values, errors='raise')
                        df[col] = df[col].fillna(fill_datetime)
                    except (ValueError, TypeError, pd.errors.ParserError):
                        pass  # Keep NaT if conversion fails

                # Case 3: Object columns (text, categorical)
                elif pd.api.types.is_object_dtype(df[col]) and fill_object_values != "":
                    # Always fill object columns with the specified value
                    df[col] = df[col].fillna(fill_object_values)

            return df

        except Exception as e:
            logging.warning(f"Missing value handling failed: {e}")
            return df

    def _clean_text_encoding(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans text columns for character encoding issues.
        """
        try:
            for col in df.select_dtypes(include=['object']).columns:
                if df[col].dtype == 'object':
                    # Handle common encoding issues
                    df[col] = df[col].astype(str)
                    # Remove or replace problematic characters
                    df[col] = df[col].str.replace(r'[^\x00-\x7F]+', '', regex=True)  # Remove non-ASCII
                    df[col] = df[col].str.replace('\x00', '', regex=False)  # Remove null bytes
                    df[col] = df[col].str.strip()  # Remove leading/trailing whitespace
                    # Limit string length for database compatibility (adjust as needed)
                    df[col] = df[col].str[:255]
            return df

        except Exception as e:
            logging.warning(f"Character encoding cleanup failed: {e}")
            return df

    def get_gads_report(self, customer_id: str, report_model: Dict[str, Any],
                        start_date: date, end_date: date,
                        filter_zero_impressions: bool = True) -> pd.DataFrame:
        """
        Retrieves GAds report data using GoogleAdsClient().get_service().search() .

        Parameters:
        - customer_id (str): The customer ID for Google Ads.
        - report_model (dict): The report model specifying dimensions, metrics, etc.
        - start_date (date): Start date for the report.
        - end_date (date): End date for the report.
        - filter_zero_impressions (bool): Whether to filter out zero impression rows

        Returns:
        - DataFrame: Pandas DataFrame containing GAds report data optimized for database storage.
        """

        response = self._get_google_ads_response(customer_id, report_model, start_date, end_date)

        result_df = self._convert_response_to_df(response, report_model)

        if not result_df.empty:

            # Filter out rows with zero impressions (configurable behavior)
            if filter_zero_impressions and "metrics.impressions" in result_df.columns:
                # Handle multiple zero representations: 0, "0", 0.0, "0.0", None, NaN
                mask = pd.to_numeric(result_df["metrics.impressions"], errors='coerce').fillna(0) != 0
                result_df = result_df.loc[mask]

            # 2. Essential data type fixes (dates and object metrics that should be numeric)
            result_df = self._fix_data_types(result_df)

            # 3. Handle missing values (after type conversion to preserve proper nulls for numerics)
            result_df = self._handle_missing_values(result_df, fill_object_values="")

            # 4. Clean text encoding for database compatibility
            result_df = self._clean_text_encoding(result_df)

        # 5. Rename columns for better readability (keeping your existing strategy)
        result_df.columns = [col.replace(".", "_").replace("segments_", "").replace(
            "adGroupCriterion_", "").replace("metrics_", "") for col in result_df.columns]

        return result_df

    # Alias to maintain compatibility with previous versions
    get_default_report = get_gads_report

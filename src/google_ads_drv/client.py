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

            # Filter out rows with zero impressions (configurable behavior)
            if "metrics.impressions" in result_df.columns:
                result_df = result_df.loc[result_df["metrics.impressions"] != 0]

            # Fill NaN values
            result_df = result_df.fillna("")

            # Rename columns for better readability
            result_df.columns = [col.replace(".", "_").replace("segments_", "").replace(
                "adGroupCriterion_", "").replace("metrics_", "") for col in result_df.columns]

            return result_df

        except Exception as e:
            raise DataProcessingError(
                "Failed to convert API response to DataFrame",
                original_error=e,
                report_name=report_model.get('report_name', 'unknown')
            ) from e

    def get_gads_report(self, customer_id: str, report_model: Dict[str, Any],
                        start_date: date, end_date: date) -> pd.DataFrame:
        """
        Retrieves GAds report data using GoogleAdsClient().get_service().search() .

        Parameters:
        - customer_id (str): The customer ID for Google Ads.
        - report_model (dict): The report model specifying dimensions, metrics, etc.
        - start_date (date): Start date for the report.
        - end_date (date): End date for the report.

        Returns:
        - DataFrame: Pandas DataFrame containing GAds report data.
        """

        response = self._get_google_ads_response(customer_id, report_model, start_date, end_date)

        result_df = self._convert_response_to_df(response, report_model)

        return result_df

    # Alias to maintain compatibility with previous versions
    get_default_report = get_gads_report

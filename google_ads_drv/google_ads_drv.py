"""
This driver module is part of an ETL project (extract, transform, load).
It's meant to be imported by main.py script and used to extract data from
Google Ads (GAds) and transform the request result to an dataframe object

https://developers.google.com/google-ads/api/reference/rpc/v20/overview
v.20250715
"""
import logging
import os
import pandas as pd
import socket
import yaml
from datetime import date, datetime, timedelta  # noqa

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf.json_format import MessageToDict


# Set timeout for all http connections
TIMEOUT_IN_SEC = 60 * 3  # seconds timeout limit
socket.setdefaulttimeout(TIMEOUT_IN_SEC)

logging.basicConfig(level=logging.INFO)


def test() -> None:
    # Function to test module using a sample report
    start_date = date.today() - timedelta(days=3)  # date(2023, 12, 1)  # noqa
    end_date = date.today() - timedelta(days=1)  # date(2023, 12, 1)  # noqa

    customer_id = "6587578899"  # Veros Hospital Verterinário Account

    report_model = GAdsReportModel.adgroup_ad_report

    with open(os.path.join("secrets", "google-ads.yaml"), "r") as f:
        secret = yaml.safe_load(f)

    gads_service = GAdsReport(secret)

    df = gads_service.get_gads_report(customer_id=customer_id, report_model=report_model,
                                      start_date=start_date, end_date=end_date)

    df.to_csv("./report_sample_gads.csv")

    logging.info(df.dtypes)
    logging.info(df)


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

    def __init__(self, client_secret: dict):
        """
        Initializes the GAdsReport instance.

        Parameters:
        - client_secret (dict): The YAML configuration for authentication.
        """
        try:
            # Initialize the Google Ads API client
            self.client = GoogleAdsClient.load_from_dict(client_secret, version="v20")

            logging.info("Google YAML credentials are valid!")
            logging.info("Successful client authentication using Google Ads API (GAds)")

        except Exception as e:
            logging.error(f"An exception of type {type(e).__name__} occurred", exc_info=True)
            raise Exception

        # Create a Google Ads API service client
        self.service = self.client.get_service("GoogleAdsService", version="v20")

    def _build_gads_query(self, report_model: dict, start_date: date, end_date: date) -> str:
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

    def _get_google_ads_response(self, customer_id: str, report_model: dict, start_date: date, end_date: date) -> dict:
        """
        Retrieves GAds report data using GoogleAdsClient().get_service().search() .

        Parameters:
        - customer_id (str): The customer ID for Google Ads.
        - report_model (dict): The report model specifying dimensions, metrics, etc.
        - start_date (date): Start date for the report.
        - end_date (date): End date for the report.

        Returns:
        - dict: GAds report data dict containing keys `results`, `totalResultsCount`, and `fieldMask`.
        """

        # Display request parameters
        print(f"Trying to get '{report_model['report_name']}' with {format(type(self.service).__name__)}()",
              "\n[ Request parameters ]",
              f"Customer_id: {customer_id}",
              f"Report_model: {report_model['report_name']}",
              f"Date range: from {start_date.isoformat()} to {end_date.isoformat()}",
              "",
              sep="\n")

        query_str = self._build_gads_query(report_model, start_date, end_date)
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
        try:
            # Execute the query to fetch the first page of data
            logging.info("Executing search request...")
            response = self.service.search(search_request)

            # Check if response has headers and results
            if hasattr(response, "field_mask") and response.total_results_count > 0:
                while True:
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

                full_response_dict["totalResultsCount"] = response.total_results_count
                full_response_dict["fieldMask"] = response_dict.get("fieldMask", "")

                logging.info(f"Finished fetching full report with {len(full_response_dict["results"])} rows \n")

            else:
                logging.info("Report has no 'results' with requested parameters\n")

            return full_response_dict

        except GoogleAdsException as e:
            logging.error("Google Ads Exception", e)
            raise

    def _convert_response_to_df(self, response: dict, report_model: dict) -> pd.DataFrame:
        """
        Converts the Google Ads API protobuf response 'MessageToDict(response._pb)' to dataFrame.

        Parameters:
        - response: The Google Ads API response in protobuf dict format.
        - report_model (dict): The custom report model specifying dimensions to guide dataframe schema.

        Returns:
        - DataFrame: Pandas DataFrame containing GAds report data.
        """

        # response_data = []

        # Iterate through the response results
        # for result in response.results:
        #     data = {}

        #     for field in report_model["select"]:
        #         # Split the field string into segments
        #         field_segments = field.split(".")
        #         value = result

        #         # Traverse through the segments to access the value
        #         for segment in field_segments:
        #             if hasattr(value, segment):
        #                 value = getattr(value, segment, None)
        #             else:
        #                 value = None
        #                 break

        #         data[field] = value

        #     response_data.append(data)

        # Create a DataFrame from the response data
        result_df = pd.json_normalize(response["results"])

        columns_to_drop = [col for col in result_df.columns if ".resourceName" in col]

        result_df = result_df.drop(columns=columns_to_drop)
        result_df = result_df.loc[result_df["metrics.impressions"] != 0]
        result_df = result_df.fillna("")
        result_df.columns = [col.replace(".", "_").replace("segments_", "").replace(
            "adGroupCriterion_", "").replace("metrics_", "") for col in result_df.columns]

        return result_df

    def get_gads_report(self, customer_id: str, report_model: dict, start_date: date, end_date: date) -> pd.DataFrame:
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


class GAdsReportModel:
    """
    GAdsReportModel class defines pre-configured report models for Google Ads (GAds).

    Report Models:
    - keyword_report: Model for querying GAds data based on keywords.
    - search_terms_report: Model for querying GAds data based on search terms.
    - insight_report: Model for querying GAds data for insights (not yet available in the Public API).
    """

    adgroup_ad_report = {
        "report_name": "adgroup_ad_report",
        "select": [
            "ad_group_ad.ad.id",
            "ad_group_ad.ad.name",
            "segments.date",
            "segments.ad_network_type",
            "campaign.advertising_channel_type",
            "campaign.id",
            "ad_group.id",
            "campaign.name",
            "ad_group.name",
            "ad_group_ad.ad.final_urls",
            "metrics.average_cpm",
            "metrics.impressions",
            "metrics.clicks",
            "metrics.ctr",
            "metrics.average_cpc",
            "metrics.cost_micros",
            "metrics.engagements",
            "metrics.engagement_rate",
            "metrics.interactions",
            "metrics.interaction_rate",
            "metrics.conversions",
            "metrics.conversions_from_interactions_rate",
            "metrics.conversions_value",
            "metrics.value_per_conversion",
            "metrics.value_per_all_conversions",
            "metrics.cost_per_conversion",
            "metrics.absolute_top_impression_percentage",
            "metrics.active_view_impressions",
            "metrics.active_view_measurable_impressions",
            "metrics.video_quartile_p100_rate",
            "metrics.video_quartile_p25_rate",
            "metrics.video_quartile_p50_rate",
            "metrics.video_quartile_p75_rate",
            "metrics.video_view_rate",
            "metrics.video_views",
            "metrics.view_through_conversions",
        ],
        "from": "ad_group_ad",
        "order_by": "metrics.impressions",
        "table_name": "olap__gads_adgroup_ad_report",
        "date_column": "date",
    }

    assetgroup_report = {
        "report_name": "assetgroup_report",
        "select": [
            "segments.date",
            "asset_group.campaign",
            "asset_group.id",
            "asset_group.name",
            "asset_group.final_urls",
            "metrics.impressions",
            "metrics.clicks",
            "metrics.ctr",
            "metrics.average_cpc",
            "metrics.cost_micros",
            "metrics.interactions",
            "metrics.interaction_rate",
            "metrics.conversions",
            "metrics.conversions_from_interactions_rate",
            "metrics.conversions_value",
            "metrics.value_per_conversion",
            "metrics.value_per_all_conversions",
            "metrics.cost_per_conversion",
        ],
        "from": "asset_group",
        "order_by": "metrics.impressions",
        "table_name": "olap__gads_assetgroup_report",
        "date_column": "date",
    }

    conversions_report = {
        "report_name": "conversions_report",
        "select": [
            "segments.date",
            "conversion_action.id",
            "conversion_action.name",
            "conversion_action.category",
            "conversion_action.origin",
            "conversion_action.type",
            "conversion_action.counting_type",
            "conversion_action.status",
            # "conversion_action.google_analytics_4_settings.event_name",
            "metrics.all_conversions",
            "metrics.all_conversions_value",
        ],
        "from": "conversion_action",
        "order_by": "metrics.all_conversions",
        "table_name": "olap__gads_conversions",
        "date_column": "date",
    }

    keyword_report = {
        "report_name": "keyword_report",
        "select": [
            "segments.date",
            # "segments.ad_network_type",
            "ad_group_criterion.keyword.text",
            "ad_group_criterion.keyword.match_type",
            "campaign.name",
            "ad_group.name",
            # "ad_group_criterion.system_serving_status",
            # "ad_group_criterion.approval_status",
            # "ad_group_criterion.final_urls",
            "metrics.historical_quality_score",
            "metrics.average_cpm",
            "metrics.impressions",
            "metrics.clicks",
            "metrics.ctr",
            "metrics.average_cpc",
            "metrics.cost_micros",
            "campaign.advertising_channel_type",
            "metrics.conversions_from_interactions_rate",
            "metrics.conversions_value",
            "metrics.conversions",
            "metrics.cost_per_conversion",
        ],
        "from": "keyword_view",
        "order_by": "metrics.impressions",
        "table_name": "olap__gads_keyword_report",
        "date_column": "date",
    }

    video_report = {
        "report_name": "video_report",
        "select": [
            "segments.date",
            "segments.ad_network_type",
            "video.title",
            "campaign.name",
            "ad_group.name",
            "metrics.average_cpm",
            "metrics.impressions",
            "metrics.clicks",
            "metrics.ctr",
            "metrics.average_cpc",
            "metrics.cost_micros",
            "campaign.advertising_channel_type",
            "metrics.conversions_from_interactions_rate",
            "metrics.conversions_value",
            "metrics.conversions",
            "metrics.cost_per_conversion",
            "metrics.engagement_rate",
            "metrics.engagements",
            "metrics.impressions",
            "metrics.value_per_all_conversions",
            "metrics.value_per_conversion",
            "metrics.video_quartile_p100_rate",
            "metrics.video_quartile_p25_rate",
            "metrics.video_quartile_p50_rate",
            "metrics.video_quartile_p75_rate",
            "metrics.video_view_rate",
            "metrics.video_views",
            "metrics.view_through_conversions",
        ],
        "from": "video",
        "order_by": "metrics.impressions",
        "table_name": "olap__gads_video_report",
        "date_column": "date",
    }

    search_terms_report = {
        "report_name": "search_terms_report",
        "select": [
            "segments.date",
            "search_term_view.search_term",
            "segments.keyword.info.match_type",
            "search_term_view.status",
            "campaign.name",
            "ad_group.name",
            "metrics.average_cpm",
            "metrics.impressions",
            "metrics.clicks",
            "metrics.ctr",
            "metrics.average_cpc",
            "metrics.cost_micros",
            "campaign.advertising_channel_type",
            "metrics.conversions_from_interactions_rate",
            "metrics.conversions_value",
            "metrics.conversions",
            "metrics.cost_per_conversion",
        ],
        "from": "search_term_view",
        "order_by": "metrics.impressions",
        "table_name": "olap__gads_search_terms_report",
        "date_column": "date",
    }


if __name__ == "__main__":
    test()

# ------------------------------------------------------------------------------------------
# END of module
# ------------------------------------------------------------------------------------------

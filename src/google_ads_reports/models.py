"""
Google Ads report models module.

This module contains pre-configured report models for different types of Google Ads reports.
"""
from typing import Any, Dict, List, Optional


class GAdsReportModel:
    """
    GAdsReportModel class defines pre-configured report models for Google Ads (GAds).

    Report Models:
    - adgroup_ad_report: Model for querying GAds data based on ad group ads.
    - assetgroup_report: Model for querying GAds data based on asset groups.
    - conversions_report: Model for querying GAds conversion data.
    - keyword_report: Model for querying GAds data based on keywords.
    - search_terms_report: Model for querying GAds data based on search terms.
    - video_report: Model for querying GAds video data.
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
            # "metrics.phone_calls",  # PROHIBITED_METRIC_IN_SELECT_OR_WHERE_CLAUSE
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
            # "metrics.phone_calls",  # PROHIBITED_METRIC_IN_SELECT_OR_WHERE_CLAUSE
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

    @classmethod
    def get_all_reports(cls) -> Dict[str, Dict[str, Any]]:
        """
        Get all available report models.

        Returns:
            Dict[str, Dict[str, Any]]: Dictionary of all report models
        """
        return {
            'adgroup_ad_report': cls.adgroup_ad_report,
            'assetgroup_report': cls.assetgroup_report,
            'conversions_report': cls.conversions_report,
            'keyword_report': cls.keyword_report,
            'video_report': cls.video_report,
            'search_terms_report': cls.search_terms_report,
        }

    @classmethod
    def get_report_by_name(cls, report_name: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific report model by name.

        Args:
            report_name (str): The name of the report model

        Returns:
            Optional[Dict[str, Any]]: The report model if found, None otherwise
        """
        all_reports = cls.get_all_reports()
        return all_reports.get(report_name)

    @classmethod
    def list_available_reports(cls) -> List[str]:
        """
        List all available report names.

        Returns:
            List[str]: List of available report names
        """
        return list(cls.get_all_reports().keys())


# Factory function for creating custom report models
def create_custom_report(report_name: str, select: List[str], from_table: str,
                         order_by: Optional[str] = None, where: Optional[str] = None,
                         table_name: Optional[str] = None,
                         date_column: str = "date") -> Dict[str, Any]:
    """
    Create a custom report model.

    Args:
        report_name (str): Name of the custom report
        select (List[str]): List of fields to select
        from_table (str): Table to query from
        order_by (Optional[str]): Field to order by (besides date)
        where (Optional[str]): Additional WHERE clause conditions
        table_name (Optional[str]): Target table name for ETL
        date_column (str): Date column name

    Returns:
        Dict[str, Any]: Custom report model configuration
    """
    report_model = {
        "report_name": report_name,
        "select": select,
        "from": from_table,
        "date_column": date_column,
    }

    if order_by:
        report_model["order_by"] = order_by

    if where:
        report_model["where"] = where

    if table_name:
        report_model["table_name"] = table_name

    return report_model

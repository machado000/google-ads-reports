"""
Custom Report Example

This example demonstrates how to create custom report models
and use them with the Google Ads driver.
"""
import logging
from datetime import date, timedelta

from google_ads_drv import GAdsReport, create_custom_report, load_credentials, setup_logging


def create_campaign_performance_report():
    """Create a custom campaign performance report."""

    # Define custom fields for campaign performance
    custom_fields = [
        "campaign.id",
        "campaign.name",
        "campaign.status",
        "campaign.advertising_channel_type",
        "segments.date",
        "metrics.impressions",
        "metrics.clicks",
        "metrics.ctr",
        "metrics.average_cpc",
        "metrics.cost_micros",
        "metrics.conversions",
        "metrics.conversions_value",
        "metrics.cost_per_conversion",
    ]

    # Create the custom report model
    campaign_report = create_custom_report(
        report_name="custom_campaign_performance",
        select=custom_fields,
        from_table="campaign",
        order_by="metrics.cost_micros",
        table_name="custom_campaign_perf"
    )

    return campaign_report


def create_ad_group_quality_report():
    """Create a custom ad group quality report."""

    quality_fields = [
        "ad_group.id",
        "ad_group.name",
        "campaign.name",
        "segments.date",
        "metrics.impressions",
        "metrics.clicks",
        "metrics.ctr",
        "metrics.average_cpc",
        "metrics.quality_score",
        "metrics.search_impression_share",
        "metrics.search_rank_lost_impression_share",
    ]

    # Create custom report with WHERE clause
    quality_report = create_custom_report(
        report_name="ad_group_quality_analysis",
        select=quality_fields,
        from_table="ad_group",
        order_by="metrics.quality_score",
        where="metrics.impressions > 100",  # Only ad groups with significant impressions
        table_name="ad_group_quality"
    )

    return quality_report


def main():
    """Main function to demonstrate custom reports."""
    # Setup logging
    setup_logging(level=logging.INFO)

    # Load credentials
    try:
        credentials = load_credentials()
    except FileNotFoundError:
        logging.error("Could not find credentials file")
        return

    # Initialize client
    gads_client = GAdsReport(credentials)

    # Configuration
    customer_id = "1234567890"  # Replace with your actual customer ID
    start_date = date.today() - timedelta(days=14)  # Last 2 weeks
    end_date = date.today() - timedelta(days=1)     # Until yesterday

    # Create and run custom reports
    custom_reports = [
        ("Campaign Performance", create_campaign_performance_report()),
        ("Ad Group Quality", create_ad_group_quality_report()),
    ]

    for report_title, report_model in custom_reports:
        try:
            logging.info(f"Running {report_title} report...")

            # Extract the data
            df = gads_client.get_gads_report(
                customer_id=customer_id,
                report_model=report_model,
                start_date=start_date,
                end_date=end_date
            )

            # Save the results
            filename = f"custom_{report_model['report_name']}_{start_date}_{end_date}.csv"
            df.to_csv(filename, index=False)

            print(f"\n{report_title} Report:")
            print(f"- Report Name: {report_model['report_name']}")
            print(f"- Rows: {len(df)}")
            print(f"- Columns: {len(df.columns)}")
            print(f"- File: {filename}")
            print(f"- Fields: {len(report_model['select'])} selected")

            if 'where' in report_model:
                print(f"- Filter: {report_model['where']}")

            # Show top performing items (first 5 rows)
            if not df.empty:
                print("- Top 5 results:")
                for i, row in df.head().iterrows():
                    if 'campaign_name' in df.columns:
                        name_col = 'campaign_name'
                    elif 'ad_group_name' in df.columns:
                        name_col = 'ad_group_name'
                    else:
                        name_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]

                    print(f"  {i+1}. {row.get(name_col, 'N/A')}")

            print("-" * 50)

        except Exception as e:
            logging.error(f"Error running {report_title} report: {e}")

    print("\nCustom report extraction completed!")


if __name__ == "__main__":
    main()

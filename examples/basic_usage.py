"""
Basic Google Ads Report Example

This example demonstrates the basic usage of the google-ads-drv package
to extract data from Google Ads API and export it to CSV.
"""
import logging
import os
from datetime import date, timedelta
from dotenv import load_dotenv

from google_ads_reports import GAdsReport, GAdsReportModel, setup_logging
from google_ads_reports.utils import (
    load_credentials,
    create_output_directory,
    format_report_filename,
    save_report_to_csv,
    get_records_info,
)


def main():
    # Setup logging
    setup_logging(level=logging.INFO)

    # Load credentials from the default location
    try:
        credentials = load_credentials()  # Will look in secrets/google-ads.yaml by default
    except Exception as e:
        logging.error("Could not find Google Ads credentials file. Please ensure you have "
                      "a google-ads.yaml file in the secrets/ directory or specify the path.")
        logging.error(f"Error: {e}")
        return

    # Initialize the Google Ads client
    gads_client = GAdsReport(client_secret=credentials)

    # Configuration
    load_dotenv()
    customer_id = os.getenv("CUSTOMER_ID") or "1234567890"  # Replace with your actual customer ID
    start_date = date.today() - timedelta(days=7)  # Last 7 days
    end_date = date.today() - timedelta(days=1)    # Until yesterday

    # Get the adgroup ad report
    report_model = GAdsReportModel.adgroup_ad_report

    try:
        # Extract the report data
        logging.info(f"Extracting '{report_model['report_name']}' for customer '{customer_id}'\n")
        response_data = gads_client.get_gads_report(customer_id, report_model,
                                                    start_date, end_date,
                                                    filter_zero_impressions=True,
                                                    column_naming="snake_case")

        # Save to file
        output_filename = format_report_filename(
            customer_id=customer_id,
            report_name=report_model['report_name'],
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            file_extension="csv"
        )

        output_dir = create_output_directory("reports_output")
        file_path = output_dir / output_filename
        save_report_to_csv(response_data, str(file_path))

        # Display basic info
        info = get_records_info(response_data)
        print("\nReport Summary:")
        print(f"- Rows: {info['shape'][0]}")
        print(f"- Columns: {info['shape'][1]}")
        print(f"- Date range: {start_date} to {end_date}")
        print(f"- Output file: {file_path}")

        # Show column names
        print("\nColumns:")
        for i, col in enumerate(info['columns'], 1):
            print(f"  {i}. {col}")

    except Exception as e:
        logging.error(f"Error extracting report: {e}")
        raise


if __name__ == "__main__":
    main()

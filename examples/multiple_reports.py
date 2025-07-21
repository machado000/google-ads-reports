"""
Multiple Reports Example

This example shows how to extract multiple types of reports
and save them in an organized directory structure.
"""
import logging
import os
from datetime import date, timedelta
from dotenv import load_dotenv
# from pathlib import Path

from google_ads_reports import (
    GAdsReport, GAdsReportModel, load_credentials, setup_logging,
    create_output_directory, format_report_filename
)


def extract_multiple_reports():
    """Extract multiple report types for a given date range."""
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
    load_dotenv()
    customer_id = os.getenv("CUSTOMER_ID") or "1234567890"  # Replace with your actual customer ID
    start_date = date.today() - timedelta(days=7)  # Last 30 days
    end_date = date.today() - timedelta(days=1)     # Until yesterday

    # Create output directory
    output_dir = create_output_directory("reports_output")

    # List of reports to extract
    reports_to_extract = [
        ("keyword_report", GAdsReportModel.keyword_report),
        ("adgroup_ad_report", GAdsReportModel.adgroup_ad_report),
        ("search_terms_report", GAdsReportModel.search_terms_report),
        ("conversions_report", GAdsReportModel.conversions_report),
    ]

    results = {}

    for report_name, report_model in reports_to_extract:
        try:
            logging.info(f"Extracting {report_name}...")

            # Extract the data
            df = gads_client.get_gads_report(customer_id, report_model,
                                             start_date, end_date,
                                             filter_zero_impressions=True)

            if df.empty:
                logging.warning(f"{report_name} returned no data")
                results[report_name] = {"status": "empty", "rows": 0}
                continue

            # Generate filename
            filename = format_report_filename(
                report_name=report_name,
                customer_id=customer_id,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat()
            )

            # Save to file
            file_path = output_dir / filename
            df.to_csv(file_path, index=False)

            results[report_name] = {
                "status": "success",
                "rows": len(df),
                "columns": len(df.columns),
                "file": str(file_path)
            }

            logging.info(f"✅ {report_name}: {len(df)} rows saved to {filename}")

        except Exception as e:
            logging.error(f"❌ Error extracting {report_name}: {e}")
            results[report_name] = {"status": "error", "error": str(e)}

    # Print summary
    print("\n" + "="*60)
    print("EXTRACTION SUMMARY")
    print("="*60)

    successful = 0
    total_rows = 0

    for report_name, result in results.items():
        status_emoji = {
            "success": "✅",
            "empty": "⚠️",
            "error": "❌"
        }.get(result["status"], "❓")

        print(f"{status_emoji} {report_name:<20} | Status: {result['status']:<8}", end="")

        if result["status"] == "success":
            print(f" | Rows: {result['rows']:<8} | Columns: {result['columns']}")
            successful += 1
            total_rows += result["rows"]
        elif result["status"] == "empty":
            print(" | No data found")
        else:
            print(f" | Error: {result.get('error', 'Unknown error')}")

    print("="*60)
    print(f"Successfully extracted: {successful}/{len(reports_to_extract)} reports")
    print(f"Total rows extracted: {total_rows:,}")
    print(f"Output directory: {output_dir}")
    print("="*60)


if __name__ == "__main__":
    extract_multiple_reports()

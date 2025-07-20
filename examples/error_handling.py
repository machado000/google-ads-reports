"""
Error Handling Example

This example demonstrates how to handle different types of errors
that can occur when using the google-ads-drv package.
"""
import logging
from datetime import date, timedelta

from google_ads_drv import (
    APIError,
    AuthenticationError,
    ConfigurationError,
    DataProcessingError,
    GAdsReport,
    GAdsReportModel,
    ValidationError,
    load_credentials,
    setup_logging,
)


def main():
    # Setup logging to see retry attempts
    setup_logging(level=logging.INFO)

    print("Google Ads Driver - Error Handling Example")
    print("=" * 50)

    # Example 1: Configuration/Credential errors
    print("\n1. Testing credential loading...")
    try:
        # Try to load from a non-existent file
        credentials = load_credentials("non_existent_file.yaml")
    except ConfigurationError as e:
        print(f"✓ Caught ConfigurationError: {e.message}")
        if e.original_error:
            print(f"  Original error: {type(e.original_error).__name__}")

    # Example 2: Load real credentials
    print("\n2. Loading real credentials...")
    try:
        credentials = load_credentials()  # Use default location
        print("✓ Credentials loaded successfully")
    except ConfigurationError as e:
        print(f"✗ Failed to load credentials: {e.message}")
        print("Please ensure you have a valid google-ads.yaml file in the secrets/ directory")
        return

    # Example 3: Authentication errors
    print("\n3. Testing authentication...")
    try:
        gads_client = GAdsReport(credentials)
        print("✓ Authentication successful")
    except AuthenticationError as e:
        print(f"✗ Authentication failed: {e.message}")
        if e.original_error:
            print(f"  Original error: {type(e.original_error).__name__}")
        return

    # Example 4: Validation errors
    print("\n4. Testing input validation...")

    # Test invalid customer ID
    try:
        invalid_customer_id = "invalid_id"
        start_date = date.today() - timedelta(days=7)
        end_date = date.today() - timedelta(days=1)
        report_model = GAdsReportModel.adgroup_ad_report

        df = gads_client.get_gads_report(
            customer_id=invalid_customer_id,
            report_model=report_model,
            start_date=start_date,
            end_date=end_date
        )
    except ValidationError as e:
        print(f"✓ Caught ValidationError: {e.message}")

    # Example 5: API errors with retry
    print("\n5. Testing API calls with valid data...")
    try:
        customer_id = "1234567890"  # Replace with your actual customer ID
        start_date = date.today() - timedelta(days=7)
        end_date = date.today() - timedelta(days=1)
        report_model = GAdsReportModel.keyword_report  # Smaller report for testing

        print(f"Attempting to fetch report for customer {customer_id}")
        print("Note: If the customer ID is invalid, you'll see retry attempts...")

        df = gads_client.get_gads_report(
            customer_id=customer_id,
            report_model=report_model,
            start_date=start_date,
            end_date=end_date
        )

        print(f"✓ Report extracted successfully: {len(df)} rows")

    except APIError as e:
        print(f"✗ API Error after retries: {e.message}")
        if e.context:
            print(f"  Context: {e.context}")
    except ValidationError as e:
        print(f"✗ Validation Error: {e.message}")
    except DataProcessingError as e:
        print(f"✗ Data Processing Error: {e.message}")

    # Example 6: General error handling pattern
    print("\n6. Recommended error handling pattern:")
    print("""
    try:
        # Your Google Ads API calls
        df = gads_client.get_gads_report(...)

    except AuthenticationError:
        # Handle credential/auth issues
        # Retry with new credentials or exit

    except ValidationError as e:
        # Handle input validation errors
        # Fix input parameters and retry

    except APIError as e:
        # Handle API errors (after retries)
        # Log error, possibly try different approach

    except DataProcessingError:
        # Handle data conversion errors
        # Possibly try with different report model

    except ConfigurationError:
        # Handle setup/config issues
        # Check configuration and retry
    """)

    print("\nError handling example completed!")


if __name__ == "__main__":
    main()

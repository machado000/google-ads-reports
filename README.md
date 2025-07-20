# Google Ads Reports Helper

A Python ETL driver for Google Ads API data extraction and transformation. Simplifies the process of extracting Google Ads data and converting it to pandas DataFrames.

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- **Simple API**: Easy-to-use interface for Google Ads data extraction
- **Multiple Report Types**: Pre-configured report models for common use cases
- **Custom Reports**: Create custom report configurations
- **Robust Error Handling**: Comprehensive error handling with retry logic
- **Pandas Integration**: Direct output to pandas DataFrames
- **Type Hints**: Full type hint support for better IDE experience

## Installation

```bash
pip install google-ads-reports
```

## Quick Start

### 1. Set up credentials

Create a `secrets/google-ads.yaml` file with your Google Ads API credentials:

```yaml
developer_token: "YOUR_DEVELOPER_TOKEN"
client_id: "YOUR_CLIENT_ID"
client_secret: "YOUR_CLIENT_SECRET"
refresh_token: "YOUR_REFRESH_TOKEN"
```
References:\
https://developers.google.com/google-ads/api/docs/get-started/introduction
https://developers.google.com/google-ads/api/docs/get-started/dev-token
https://developers.google.com/workspace/guides/create-credentials#service-account


### 2. Basic usage

```python
from datetime import date, timedelta
from google_ads_reports import GAdsReport, GAdsReportModel, load_credentials

# Load credentials
credentials = load_credentials()
client = GAdsReport(credentials)

# Configure report parameters
customer_id = "1234567890"
start_date = date.today() - timedelta(days=7)
end_date = date.today() - timedelta(days=1)

# Extract report data
df = client.get_gads_report(
    customer_id=customer_id,
    report_model=GAdsReportModel.keyword_report,
    start_date=start_date,
    end_date=end_date
)

# Save to CSV
df.to_csv("keyword_report.csv", index=False)
```

## Available Report Models

- `GAdsReportModel.adgroup_ad_report` - Ad group ad performance
- `GAdsReportModel.keyword_report` - Keyword performance
- `GAdsReportModel.search_terms_report` - Search terms analysis
- `GAdsReportModel.conversions_report` - Conversion tracking
- `GAdsReportModel.video_report` - Video ad performance
- `GAdsReportModel.assetgroup_report` - Asset group performance

## Custom Reports

Create custom report configurations:

```python
from google_ads_reports import create_custom_report

custom_report = create_custom_report(
    report_name="campaign_performance",
    select=[
        "campaign.name",
        "campaign.status", 
        "segments.date",
        "metrics.impressions",
        "metrics.clicks",
        "metrics.cost_micros"
    ],
    from_table="campaign",
    where="metrics.impressions > 100"
)

df = client.get_gads_report(customer_id, custom_report, start_date, end_date)
```

## Error Handling

The package provides specific exception types for different scenarios:

```python
from google_ads_reports import (
    GAdsReport, 
    AuthenticationError, 
    ValidationError, 
    APIError,
    DataProcessingError,
    ConfigurationError
)

try:
    df = client.get_gads_report(customer_id, report_model, start_date, end_date)
except AuthenticationError:
    # Handle credential issues
    pass
except ValidationError:
    # Handle input validation errors
    pass
except APIError:
    # Handle API errors (after retries)
    pass
```

## Examples

Check the `examples/` directory for comprehensive usage examples:

- `basic_usage.py` - Simple report extraction
- `multiple_reports.py` - Batch report processing  
- `custom_reports.py` - Custom report creation
- `error_handling.py` - Error handling patterns

## Configuration

### Retry Settings

API calls automatically retry on transient errors with configurable settings:

- **Max attempts**: 3 (default)
- **Base delay**: 1 second
- **Backoff factor**: 2x exponential
- **Max delay**: 30 seconds

### Logging

Configure logging level:

```python
from google_ads_reports import setup_logging
import logging

setup_logging(level=logging.DEBUG)  # Enable debug logging
```

## Requirements

- Python 3.9+
- pandas >= 2.0.0
- google-ads >= 24.0.0
- PyYAML >= 6.0.0

## Development

For development installation:

```bash
git clone https://github.com/machado000/google-ads-reports
cd google-ads-reports
pip install -e ".[dev]"
```

## License

MIT License. See [LICENSE](LICENSE) file for details.

## Support

- [Documentation](https://github.com/machado000/google-ads-reports#readme)
- [Issues](https://github.com/machado000/google-ads-reports/issues)
- [Examples](examples/)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

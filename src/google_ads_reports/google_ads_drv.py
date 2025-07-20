"""
Legacy compatibility module for google_ads_drv.

This module maintains backward compatibility while the codebase transitions
to the new modular structure. It imports and re-exports everything from
the new modules.

DEPRECATED: This module is kept for backward compatibility.
Please use the new modular imports:
- from google_ads_reports.client import GAdsReport
- from google_ads_reports.models import GAdsReportModel
"""
import warnings

# Import everything from the new modular structure
from .client import GAdsReport  # noqa: F401
from .models import GAdsReportModel  # noqa: F401
from .utils import setup_logging

# Show deprecation warning
warnings.warn(
    "Direct import from google_ads_reports.google_ads_drv is deprecated. "
    "Please use 'from google_ads_reports import GAdsReport, GAdsReportModel' instead.",
    DeprecationWarning,
    stacklevel=2
)

# Set up basic logging for backward compatibility
setup_logging()


# For backward compatibility, expose the test function
def test():
    """
    DEPRECATED: Test function moved to examples.

    Please use the example scripts in the examples/ directory instead:
    - examples/basic_usage.py
    - examples/multiple_reports.py
    - examples/custom_reports.py
    """
    warnings.warn(
        "The test() function is deprecated. Please use the example scripts "
        "in the examples/ directory instead.",
        DeprecationWarning,
        stacklevel=2
    )

    print("The test() function has been moved to example scripts.")
    print("Please check the examples/ directory for:")
    print("- examples/basic_usage.py - Basic report extraction")
    print("- examples/multiple_reports.py - Multiple report extraction")
    print("- examples/custom_reports.py - Custom report creation")


if __name__ == "__main__":
    test()

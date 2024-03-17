# Standard library
import os

# Third party
import requests

# Local
from ..settings import Settings
from ..util.get_logger import get_logger


class Polygon:
    """Handle requests to Polygon API.
    https://polygon.io/

    * Export POLYGON_KEY=YOUR_KEY as env variable
    """

    def __init__(self, settings: Settings, api_calls_per_min: int = 5):
        """Initialize cretencials and settings."""

        self.logger = get_logger(__name__, settings)
        api_key = os.getenv("POLYGON_KEY")
        self.api_key_url = f"&apiKey={api_key}"
        self.api_calls_per_min = api_calls_per_min
        self.base_url = settings.BASE_URL
        self.endpoints = settings.ENDPOINTS

    def get_grouped_daily(self, date: str, adjusted: bool = True) -> dict:
        """Return the daily open, high, low, and close (OHLC).
        for the entire stocks/equities markets.

        -- date format yyyy-mm-dd
        """

        adjusted_query = "?adjusted=true" if adjusted else "?adjusted=false"
        url = (
            f"{self.base_url}"
            f"{self.endpoints.get('grouped_daily_endpoint')}"
            f"{date}"
            f"{adjusted_query}"
            f"{self.api_key_url}"
        )

        resp = requests.get(url)
        return resp.json()

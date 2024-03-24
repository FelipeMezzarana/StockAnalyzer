# Standard library
import os

# Third party
import requests
from time import perf_counter, sleep

# Local
from ..settings import Settings
from ..util.get_logger import get_logger

class Polygon:
    """Handle requests to Polygon API.
    https://polygon.io/

    * Export POLYGON_KEY=YOUR_KEY as env variable
    """

    def __init__(self, settings: Settings):
        """Initialize cretencials and settings."""

        self.logger = get_logger(__name__, settings)
        api_key = os.getenv("POLYGON_KEY")
        self.api_key_url = f"&apiKey={api_key}"
        self.base_url = settings.BASE_URL
        self.endpoints = settings.ENDPOINTS
        self.api_calls_per_min = settings.POLYGON_CALLS_PER_MIN
        self.api_sleep_time = 60/self.api_calls_per_min
        self.last_request = 0 # Placeholder

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

        self.check_api_limit()
        resp = requests.get(url)
        # We need to make sure that request is successful
        tries = 0
        while resp.status_code != 200: # pragma: no cover
            if tries == 3:
                raise Exception(f"3 unsuccessful attempts to request {url=}")
            tries += 1
            sleep(5)
            resp = requests.get(url)
        self.logger.info(f"Extracted {date=}")
        return resp.json()

    def check_api_limit(self):
        """Check API rate limit.
        If reached, sleep enough before the next request.
        """

        now = perf_counter()
        if now - self.last_request < 12:
            sleep_time = 12 - (now - self.last_request)
            self.logger.debug(f"{sleep_time=}")
            sleep(sleep_time)
            # Update last request time
            self.last_request = perf_counter()
        else:
            self.last_request = perf_counter()
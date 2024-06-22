# Standard library
import os
from time import perf_counter, sleep

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

    def __init__(self, settings: Settings):
        """Initialize cretencials and settings."""

        self.logger = get_logger(__name__, settings)
        api_key = os.getenv("POLYGON_KEY")
        self.api_key_url = f"&apiKey={api_key}"
        self.base_url = settings.POLYGON["BASE_URL"]
        self.endpoints = settings.POLYGON["ENDPOINTS"]
        self.api_calls_per_min: int = settings.POLYGON["POLYGON_CALLS_PER_MIN"]
        self.api_sleep_time = 60 / self.api_calls_per_min
        self.last_request = 0  # Placeholder

    def request(self, url: str) -> dict:
        """Return the Polygon request response.
        Handle API Limit

        url -- request endpoint without API key
        """

        url_with_key = url + f"{self.api_key_url}"
        self.check_api_limit()
        resp = requests.get(url_with_key)
        # We need to make sure that request is successful
        tries = 0
        while resp.status_code != 200:  # pragma: no cover
            if tries == 3:
                raise Exception(f"3 unsuccessful attempts to request {url=}")
            tries += 1
            sleep(5)
            resp = requests.get(url_with_key)
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

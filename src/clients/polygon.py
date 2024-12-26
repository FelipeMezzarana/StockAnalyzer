# Standard library
import os
from time import perf_counter, sleep

# Third party
import requests

# Local
from ..exceptions import MaxRetriesExceededError, MissingAPIKeyError
from ..settings import Settings
from ..utils.decorators import singleton
from ..utils.get_logger import get_logger


@singleton
class Polygon:
    """Handle requests to Polygon API.
    https://polygon.io/

    * Export POLYGON_KEY=YOUR_KEY as env variable
    """

    def __init__(self, settings: Settings):
        """Initialize credentials and settings."""

        self.logger = get_logger(__name__, settings)
        api_key = os.getenv("POLYGON_KEY")
        if not api_key:  # pragma: no cover
            raise MissingAPIKeyError(
                "POLYGON_KEY", "https://polygon.io/dashboard/signup?redirect=%2Fdashboard%3F"
            )
        self.api_key_url = f"&apiKey={api_key}"

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
            tries += 1
            self.logger.info(f"Request {tries} failed: {resp.status_code=}, {resp.json()} ")
            if tries > 3:
                raise MaxRetriesExceededError(url, 3)
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
            self.logger.debug(f"{sleep_time=:.2f}")
            sleep(sleep_time)
            # Update last request time
            self.last_request = perf_counter()
        else:
            self.last_request = perf_counter()

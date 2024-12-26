# Standard library
import os
from time import sleep

# Third party
import requests

# Local
from ..exceptions import MaxRetriesExceededError, MissingAPIKeyError
from ..settings import Settings
from ..utils.get_logger import get_logger


class Fred:
    """Handle requests to FRED API.
    Hosted by the Economic Research Division of the Federal Reserve Bank of St. Louis
    https://fred.stlouisfed.org/docs/api/fred/

    * Export FRED_KEY=YOUR_KEY as env variable
    """

    def __init__(self, settings: Settings):
        """Initialize credentials and settings."""

        self.logger = get_logger(__name__, settings)
        api_key = os.getenv("FRED_KEY")
        if not api_key:  # pragma: no cover
            raise MissingAPIKeyError(
                "FRED_KEY", "https://fred.stlouisfed.org/docs/api/api_key.html"
            )
        self.api_key_url = f"&api_key={api_key}&file_type=json"
        self.base_url = settings.FRED["BASE_URL"]
        self.endpoints = settings.FRED["ENDPOINTS"]

    def request(self, url: str) -> dict:
        """Return the FRED request response.

        url -- request endpoint without API key
        """

        url_with_key = url + f"{self.api_key_url}"
        resp = requests.get(url_with_key)
        # We need to make sure that request is successful
        tries = 0
        while resp.status_code != 200:  # pragma: no cover
            if tries == 3:
                raise MaxRetriesExceededError(url, 3)
            tries += 1
            sleep(5)
            resp = requests.get(url_with_key)
        return resp.json()

# Standard library
from time import sleep
from typing import Dict

# Third party
import requests

# Local
from ..abstract.step import Step
from ..exceptions import MaxRetriesExceededError
from ..settings import Settings


class WikipediaExtractor(Step):
    """Extract a single html page from Wikipedia."""

    def __init__(self, page: str, previous_output: Dict, settings: Settings) -> None:
        """Init class.

        Args:
            page: name of the Wikipedia page to extract.
        """
        super(WikipediaExtractor, self).__init__(__name__, previous_output, settings)
        self.url = "https://en.wikipedia.org/w/api.php"
        self.headers = {
            "User-Agent": "StockAnalyzer/0.0 (https://github.com/FelipeMezzarana/StockAnalyzer"
        }
        self.params = {"action": "parse", "page": page, "format": "json", "prop": "text"}

    def request(self) -> str:
        """Return html for the url."""

        response = requests.get(self.url, headers=self.headers, params=self.params)
        req_count = 1
        while response.status_code != 200:  # pragma: no cover
            if req_count > 3:
                raise MaxRetriesExceededError(self.url, 3)
            self.logger.info(
                f"Request {req_count}/3 to {self.url} Failed. {response.status_code=}."
                " Waiting 5 sec. and trying again."
            )
            sleep(5)
            response = requests.get(self.url, headers=self.headers, params=self.params)
            req_count += 1

        return response.json()["parse"]["text"]["*"]

    def run(self) -> tuple[bool, Dict]:
        """Extract html and save in .txt file."""

        html = self.request()
        file_path = "temp/html_temp.txt"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html)
        self.output["file_path"] = file_path

        return True, self.output

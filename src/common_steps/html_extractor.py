
import requests
from ..settings import Settings
from time import sleep
from ..abstract.step import Step
from typing import Dict


class HtmlExtractor(Step):
    """Extract a single html page.
    """

    def __init__(self,url: str, previous_output: Dict, settings: Settings) -> None:
        """Init class."""
        super(HtmlExtractor, self).__init__(__name__, previous_output, settings)
        self.url = url

    def request(self) -> str:
        """Return html for the url."""

        response = requests.get(self.url)
        req_count = 1
        while response.status_code != 200: # pragma: no cover
            if req_count >3:
                raise Exception("Request failed 3 times.")
            self.logger.info(
                f"Request {req_count}/3 to {self.url} Failed. {response.status_code=}."
                " Waiting 5 sec. and trying again."
                )
            sleep(5)
            response = requests.get(self.url)
            req_count += 1

        return response.text
    
    def run(self) -> tuple[bool, Dict]:
        """Extract html and save in .txt file. """

        html = self.request()
        file_path = "temp/html_temp.txt"
        with open(file_path, 'w', encoding='utf-8') as f: 
            f.write(html) 
        self.output["file_path"] = file_path

        return True, self.output
    


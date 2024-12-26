# Standard library

# Standard library
from datetime import datetime
from typing import Any, Dict, List

# Third party
from bs4 import BeautifulSoup

# Local
from ....abstract.step import Step
from ....settings import Settings
from ....utils.csv_handler import append_to_file, clean_temp_file


class SP500Transformer(Step):
    """Transform html SP500 table into csv."""

    def __init__(self, previous_output: dict, settings: Settings):
        """Init class."""
        super(SP500Transformer, self).__init__(__name__, previous_output, settings)

        self.file_path: str = self.previous_output["file_path"]
        self.pipeline: str = self.settings.pipeline
        self.pipeline_table: Dict[str, Any] = self.settings.PIPELINE_TABLE
        self.fields_mapping: Dict = self.pipeline_table["fields_mapping"]
        self.required_fields: list = self.pipeline_table["required_fields"]

    def get_bs4_element(self) -> BeautifulSoup:
        """Read the txt html and parse with BeautifulSoup."""

        with open(self.file_path, "r") as f:
            html = f.read()
        return BeautifulSoup(html, "html.parser")

    def check_header(self, bs_table: BeautifulSoup):
        """Return header.
        Check if header matches expectation.
        """
        header = [
            i.text.replace("\n", "").strip() for i in bs_table.find_all("tr")[0].find_all("th")
        ]
        expected_header = [
            "Symbol",
            "Security",
            "GICS Sector",
            "GICS Sub-Industry",
            "Headquarters Location",
            "Date added",
            "CIK",
            "Founded",
        ]

        if header != expected_header:  # pragma: no cover
            self.logger.critical(f"Unexpected Header: {header=}. {expected_header=}")
            self.logger.info("Trying to continue with expected.")
            return expected_header
        else:
            self.logger.info("Header according to expected.")
            return header

    def get_sp500_table(self, bs: BeautifulSoup) -> tuple[List[Dict], List[str]]:
        """Extract sp500 table from bs element."""

        s_p_500_table = bs.find_all("table", {"class": "wikitable sortable sticky-header"})[0]
        header = self.check_header(s_p_500_table)
        # Extra fields
        header.append("url")

        rows = []
        c = 0
        for tr in s_p_500_table.find_all("tr"):
            row = [td.text.replace("\n", "").strip() for td in tr.find_all("td")]
            try:
                ticker_url = tr.find_all("a", {"class": "external text"}, href=True)[0].get("href")
            except Exception as e:
                ticker_url = f"not_found. {e=}"

            if row and len(row) == 8:
                row.append(ticker_url)
                dict_row = {k: v for k, v in zip(header, row)}
                dict_row["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                rows.append(dict_row)
            c += 1
        header.append("updated_at")

        return rows, header

    def run(self, clean_file: bool = True):
        """Run validation step."""

        self.output["file_path"] = "temp/sp500_basic_details_temp.csv"
        bs_element = self.get_bs4_element()
        sp500_table, header = self.get_sp500_table(bs_element)
        append_to_file(self.output["file_path"], sp500_table, header)
        if clean_file:  # pragma: no cover
            clean_temp_file(self.previous_output["file_path"])

        return True, self.output

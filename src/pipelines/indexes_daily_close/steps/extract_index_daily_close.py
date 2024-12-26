# Standard library
import re
from datetime import datetime, timedelta

# Local
from ....abstract.step import Step
from ....clients.fred import Fred
from ....settings import Settings
from ....utils.csv_handler import append_to_file


class IndexDailyCloseExtractor(Step):
    """Extract indexes daily close values from Fred API.
    indexes list in settings.py
    """

    def __init__(self, previous_output: dict, settings: Settings):
        """Initiate clients and settings."""
        super(IndexDailyCloseExtractor, self).__init__(__name__, previous_output, settings)

        self.settings = settings
        self.indexes_last_update: dict = previous_output.get("indexes_last_update", {})
        self.base_url = self.settings.FRED["BASE_URL"]
        self.endpoints: dict = self.settings.FRED["ENDPOINTS"]
        self.indexes = settings.FRED["INDEXES"]

    def build_request(self, index: str) -> str:
        """Return url to request daily open, high, low, and close (OHLC).
        for the entire stocks/equities markets.

        -- request_date format yyyy-mm-dd
        """

        index_last_update_str = self.indexes_last_update[index]
        index_last_update = datetime.strptime(index_last_update_str, "%Y-%m-%d")
        observation_start = (index_last_update + timedelta(days=1)).strftime("%Y-%m-%d")

        url = (
            f"{self.base_url}"
            f"{self.endpoints.get('index_daily_close')}"
            f"series_id={index}"
            f"&observation_start={observation_start}"
        )

        return url

    def get_index_daily_close(self, index: str) -> None:
        """Get daily close data for <index>."""

        fred = Fred(self.settings)
        url = self.build_request(index)
        file_path = "temp/indexes_daily_close_temp.csv"
        self.output["file_path"] = file_path
        resp = fred.request(url)
        data = resp.get("observations")
        if data:
            self.logger.info(f"{index=} | Results: {resp.get('count')}")
            enriched_data = self._raw_enrich(data, index)
            append_to_file(file_path, enriched_data)

    def _raw_enrich(self, data: list[dict], index: str) -> list[dict]:
        """initial enrichment."""

        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        enriched_data = []
        for index_dict in data:
            enriched_index_dict = {}
            enriched_index_dict["date"] = index_dict["date"]
            enriched_index_dict["index"] = index
            if re.search(r"\d", index_dict["value"]):
                enriched_index_dict["value"] = float(index_dict["value"])
            else:
                enriched_index_dict["value"] = None
            enriched_index_dict["updated_at"] = updated_at
            enriched_data.append(enriched_index_dict)

        return enriched_data

    def run(self):
        """Run step."""
        for index in self.indexes:
            self.get_index_daily_close(index)

        return True, self.output

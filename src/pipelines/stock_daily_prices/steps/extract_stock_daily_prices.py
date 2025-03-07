# Standard library
from datetime import datetime, timedelta

# Local
from ....abstract.client import Client
from ....abstract.step import Step
from ....clients.polygon import Polygon
from ....settings import Settings
from ....utils.csv_handler import append_to_file
from ....utils.sql_handler import SQLHandler


class StockDailyPriceExtractor(Step):
    """Extract daily data from polygon API."""

    def __init__(self, previous_output: dict, settings: Settings, client: Client):
        """Initiate clients and settings.

        max_days_hist -- max historical data covered by API plan, in days.
        """
        super(StockDailyPriceExtractor, self).__init__(__name__, previous_output, settings)

        self.settings = settings
        self.sqlite_client = SQLHandler(self.settings, client)
        self.max_days_hist = self.settings.POLYGON["POLYGON_MAX_DAYS_HIST"]
        self.base_url = self.settings.POLYGON["BASE_URL"]
        self.endpoints: dict = self.settings.POLYGON["ENDPOINTS"]

    def get_last_date(self) -> str:
        """Return max date [%Y-%m-%d] for stock_daily_prices table.
        Query SQLite DB defined in src.settings.
        """
        table_name = (
            self.settings.PIPELINE_TABLE["schema"] + "." + self.settings.PIPELINE_TABLE["name"]
        )
        is_successful, last_date = self.sqlite_client.query(f"SELECT MAX(DATE) FROM {table_name}")

        last_update_date = last_date[0][0] if last_date else None
        if is_successful and last_update_date:  # pragma: no cover
            if isinstance(last_update_date, datetime):
                last_update_date = last_update_date.strftime("%Y-%m-%d")
            return last_update_date
        else:  # pragma: no cover
            max_hist_available = (
                datetime.today() - timedelta(days=self.max_days_hist + 1)
            ).strftime("%Y-%m-%d")
            if last_date:
                return max_hist_available
            else:
                self.logger.info(f"Table {table_name} empty.")
                return max_hist_available

    def build_request(self, request_date) -> str:
        """Return url to request daily open, high, low, and close (OHLC).
        for the entire stocks/equities markets.

        -- request_date format yyyy-mm-dd
        """

        url = (
            f"{self.base_url}"
            f"{self.endpoints.get('stock_daily_prices_endpoint')}"
            f"{request_date}?adjusted=true"
        )
        return url

    def get_stock_daily_prices(self, last_date: str, avoid_weeknds: bool = True):
        """Get grouped daily datafor STOCK_DAILY_PRICES table.

        last_date -- last date updated(format yyyy-mm-dd)
        end_date -- end of period (format yyyy-mm-dd)
        """

        polygon_client = Polygon(self.settings)
        date_obj = datetime.strptime(last_date, "%Y-%m-%d")
        request_date = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")

        file_path = "temp/stock_daily_prices_temp.csv"
        self.output["file_path"] = file_path
        api_call_count, row_count = 0, 0
        while request_date != self.settings.POLYGON["POLYGON_UPDATE_UNTIL"]:
            if not avoid_weeknds or not self._is_weekend(request_date):
                url = self.build_request(request_date)
                result = polygon_client.request(url)
                data = result.get("results")
                if data:
                    row_count += result.get("resultsCount", 0)
                    self.logger.info(f"{row_count=}")
                    enriched_data = self._raw_enrich(data, request_date)
                    append_to_file(file_path, enriched_data)
            date_obj = datetime.strptime(request_date, "%Y-%m-%d")
            request_date = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
            self.logger.info(f"Request Successful. {request_date=}")
            api_call_count += 1
        self.logger.info(f"Update Finished. {api_call_count=} {row_count=}")

    def _raw_enrich(self, data: list[dict], request_date: str) -> list[dict]:
        """initial enrichment."""

        enriched_data = []
        for stock_dict in data:
            enriched_stock_dict = stock_dict.copy()
            # int timestamp to datetime
            enriched_stock_dict["t"] = datetime.utcfromtimestamp(stock_dict["t"] / 1000)
            # Add extra filds
            enriched_stock_dict["date"] = request_date
            enriched_stock_dict["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            enriched_data.append(enriched_stock_dict)

        return enriched_data

    def _is_weekend(self, date: str) -> bool:
        """Check if string date is weekend.
        date -- format yyyy-mm-dd
        """

        data_obj = datetime.strptime(date, "%Y-%m-%d")
        if data_obj.weekday() in [5, 6]:
            return True
        else:
            return False

    def run(self):
        """Run step."""

        last_update_date = self.get_last_date()
        self.logger.info(f"{last_update_date=}")
        self.get_stock_daily_prices(last_update_date)

        return True, self.output

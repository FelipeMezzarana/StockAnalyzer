# Standard library
import random
from datetime import datetime

# Local
from ....abstract.client import Client
from ....abstract.step import Step
from ....clients.polygon import Polygon
from ....settings import Settings
from ....utils.csv_handler import append_to_file
from ....utils.sql_handler import SQLHandler


class TickerBasicDetailsExtractor(Step):
    """Extract daily data from polygon API."""

    def __init__(self, previous_output: dict, settings: Settings, client: Client):
        """Initiate clients and settings.

        max_days_hist -- max historical data covered by API plan, in days.
        """
        super(TickerBasicDetailsExtractor, self).__init__(__name__, previous_output, settings)

        self.settings = settings
        self.sqlite_client = SQLHandler(self.settings, client)
        self.max_pagination = self.settings.POLYGON["MAX_PAGINATION"]
        self.max_days_hist = self.settings.POLYGON["POLYGON_MAX_DAYS_HIST"]
        self.base_url = self.settings.POLYGON["BASE_URL"]
        self.endpoints = self.settings.POLYGON["ENDPOINTS"]

    def get_required_tickers(self) -> tuple[list[str], list[str]]:
        """Return required tickers list.
        required = registered in STOCK_DAILY_PRICES table minus registered in STOCK_COMPANY_DETAILS.
        Query SQLite DB defined in src.settings.
        """

        _, all_tickers = self.sqlite_client.query(
            "SELECT DISTINCT(exchange_symbol) FROM BRONZE_LAYER.STOCK_DAILY_PRICES"
        )

        _, registered_tickers = self.sqlite_client.query(
            "SELECT DISTINCT(exchange_symbol) FROM BRONZE_LAYER.STOCK_COMPANY_DETAILS"
        )
        registered_tickers_list = [r[0] for r in registered_tickers]
        required_tickers = [t[0] for t in all_tickers if t[0] not in registered_tickers_list]
        self.logger.info(
            f"all_tickers: {len(all_tickers)} | "
            f"registered_tickers: {len(registered_tickers_list)} | "
            f"required_tickers: {len(required_tickers)}"
        )
        return required_tickers, registered_tickers_list

    def _get_initial_url(self):
        """Build initial url request with random sorting order."""

        sorting_op = [
            "ticker",
            "name",
            "market",
            "locale",
            "primary_exchange",
            "type",
            "currency_symbol",
            "cik",
            "composite_figi",
            "share_class_figi",
            "last_updated_utc",
        ]

        sort_by = random.choice(sorting_op)
        self.logger.info(f"{sort_by=}")
        initial_url = (
            f"{self.base_url}"
            f"{self.endpoints.get('stock_company_details_endpoint')}"
            f"&sort={sort_by}"
            "&market=stocks"
        )

        return initial_url

    def update_stock_company_details(
        self, required_tickers: list[str], registered_tickers: list[str]
    ):
        """Update missing tickers in STOCK_COMPANY_DETAILS table.

        required_tickers -- Missing tickers
        registered_tickers -- Existing tickers
        """

        polygon_client = Polygon(self.settings)

        file_path = "temp/stock_company_details_temp.csv"
        self.output["file_path"] = file_path
        header = list(self.settings.PIPELINE_TABLE["fields_mapping"].keys())
        api_call_count, row_count = 0, 0

        initial_url = self._get_initial_url()
        result = polygon_client.request(initial_url)
        while required_tickers:
            api_call_count += 1
            data = result.get("results")
            if data:
                # filter registered
                required_data = [t for t in data if t["ticker"] not in registered_tickers]
                # Update requirements
                tickers = [t.get("ticker") for t in data]
                required_tickers = [r for r in required_tickers if r not in tickers]
                self.logger.debug(
                    f"required_tickers: {len(required_tickers)} "
                    f"data: {len(data)} "
                    f"required_data: {len(required_data)}"
                )
            else:  # pragma: no cover
                self.logger.info("Request returned no data.")
                required_data = None

            if required_data:
                enriched_data = self._raw_enrich(required_data)
                append_to_file(file_path, enriched_data, header)
                row_count += len(required_data)
            # Next page
            next_url = result.get("next_url")
            if not next_url:  # pragma: no cover
                self.logger.info("next_url not found.")
                break
            if api_call_count > self.max_pagination:  # pragma: no cover
                self.logger.info(f"{self.max_pagination=} reached. stopping requests.")
                break
            result = polygon_client.request(next_url)

        self.logger.info(f"Update Finished. {api_call_count=} {row_count=}")

    def _raw_enrich(self, data: list[dict]) -> list[dict]:
        """initial enrichment."""

        enriched_data = []
        for stock_dict in data:
            enriched_stock_dict = stock_dict.copy()
            # Add extra fields
            enriched_stock_dict["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            enriched_data.append(enriched_stock_dict)

        return enriched_data

    def run(self):
        """Run step."""

        required_tickers, registered_tickers = self.get_required_tickers()
        if not required_tickers:  # pragma: no cover
            self.output["file_path"] = "no_file_flagg"
            return True, self.output
        self.update_stock_company_details(required_tickers, registered_tickers)

        return True, self.output

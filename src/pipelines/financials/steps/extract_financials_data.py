# Standard library
from datetime import datetime

# Local
from ....abstract.step import Step
from ....clients.polygon import Polygon
from ....settings import Settings
from ....utils.csv_handler import append_to_file


class FinancialsExtractor(Step):
    """Extract financials data from polygon."""

    def __init__(self, previous_output: dict, settings: Settings):
        """Initiate clients and settings."""
        super(FinancialsExtractor, self).__init__(__name__, previous_output, settings)

        self.settings = settings
        self.pipeline_tables = self.settings.PIPELINE_TABLE
        self.base_url = self.settings.POLYGON["BASE_URL"]
        self.endpoint: dict = self.settings.POLYGON["ENDPOINTS"]["financials_endpoint"]
        self.polygon_client = Polygon(settings)
        self.is_integration_test = settings.is_integration_test

    def request_ticker(self, ticker: str) -> dict:
        """Return Financials endpoint response for ticker."""

        url = f"{self.base_url}{self.endpoint}ticker={ticker}&limit=100&include_sources=true"
        self.logger.debug(f"request to: {url}")
        resp = self.polygon_client.request(url)

        return resp

    def filter_results(self, data: list[dict], ticker: str) -> list[dict]:
        """Filter results that already exists in db."""

        ticker_last_date_str = self.previous_output["required_tickers"][ticker]["last_date"]
        filtered_results = []
        for result in data:
            end_date = datetime.strptime(result["end_date"], "%Y-%m-%d")
            ticker_last_date = datetime.strptime(ticker_last_date_str, "%Y-%m-%d")
            if end_date > ticker_last_date:
                filtered_results.append(result)

        return filtered_results

    def enrich_results(self, results: list[dict], ticker: str) -> list[dict]:
        """Enrich result with metadata."""

        enriched_results = []
        for result in results:
            enriched_result = result.copy()
            # Add extra fields
            enriched_result["exchange_symbol_search"] = ticker
            enriched_result["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            enriched_results.append(enriched_result)

        return enriched_results

    def map_to_file(self, data: list[dict], table: str):
        """Map data according to settings and append to file."""

        # Mapping
        table_config = [i for i in self.pipeline_tables if i["name"] == table][0]
        endpoint_path = table_config["endpoint_path"]
        table_fields_mapping = list(table_config["fields_mapping"].keys())
        mapped_data_list = []
        for row in data:
            financial_report = row["financials"].get(endpoint_path)
            if financial_report:
                metadata = {
                    "start_date": row.get("start_date"),
                    "end_date": row.get("end_date"),
                    "timeframe": row.get("timeframe"),
                    "fiscal_period": row.get("fiscal_period"),
                    "fiscal_year": row.get("fiscal_year"),
                    "exchange_symbol_search": row.get("exchange_symbol_search"),
                    "tickers": row.get("tickers"),
                    "company_name": row.get("company_name"),
                    "updated_at": row.get("updated_at"),
                }
                metadata_fields = list(metadata.keys())
                financial_data = {}
                for field in [f for f in table_fields_mapping if f not in metadata_fields]:
                    if financial_report.get(field):
                        financial_data[field] = financial_report.get(field).get("value")
                    else:
                        financial_data[field] = None
                metadata.update(financial_data)
                mapped_data_list.append(metadata)

        return mapped_data_list

    def run(self) -> tuple[bool, dict]:  # pragma: no cover
        """Run step."""

        self.output["files_path"] = {}
        tables = [i["name"] for i in self.pipeline_tables]
        for table in tables:
            file_path = f"temp/financials_{table}_temp.csv"
            self.output["files_path"].update({table: file_path})

        required_tickers = list(self.previous_output["required_tickers"].keys())
        # Workaround to limit number of requests in integration tests
        if self.is_integration_test:  # pragma no cover
            required_tickers = required_tickers[:3]
        # Request data for each ticker.
        # Each request return all reports (for all financials tables) for the ticker.
        for ticker in required_tickers:
            data = self.request_ticker(ticker)
            if data.get("results"):
                # Filter results that already exists in db
                filtered_results = self.filter_results(data["results"], ticker)
                if filtered_results:
                    # Enrich results with metadata
                    enriched_results = self.enrich_results(filtered_results, ticker)
                    for table in tables:
                        self.logger.debug(f"Mapping data to {table} table")
                        mapped_data_list = self.map_to_file(enriched_results, table)
                        file_path = self.output["files_path"][table]
                        if mapped_data_list:
                            append_to_file(file_path, mapped_data_list)

        return True, self.output

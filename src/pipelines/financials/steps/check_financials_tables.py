# Standard library
from datetime import datetime, timedelta

# Local
from ....abstract.client import Client
from ....abstract.step import Step
from ....settings import Settings
from ....utils.sql_handler import SQLHandler


class FinancialsChecker(Step):
    """Check status of Financials tables."""

    def __init__(self, previous_output: dict, settings: Settings, client: Client):
        """Init class."""
        super(FinancialsChecker, self).__init__(__name__, previous_output, settings)

        # The pipeline has 4 tables, but we only need one to check status
        self.pipeline_table = settings.PIPELINE_TABLE[0]
        self.sqlite_client = SQLHandler(settings, client, table_config=self.pipeline_table)
        self.table = self.pipeline_table["name"]

    def get_last_update(self, required_tickers: list[str]) -> dict:
        """Return last update date each ticker in required_tickers."""

        is_successful, result = self.sqlite_client.query(
            "SELECT exchange_symbol_search, max(end_date) as last_end_date "
            f"FROM {self.table} GROUP BY exchange_symbol_search"
        )
        if is_successful:
            last_update = dict(result)
            # Results are usually released quarterly.
            # So we will look for reports after 3 months from the last end date
            financials_last_update = {
                k: last_update.get(k, datetime(1600, 1, 1)) for k in required_tickers
            }
            filtered_next_update = {}
            today = datetime.now()
            for k, v in financials_last_update.items():
                if isinstance(v, datetime):
                    next_date = v + timedelta(days=90)
                    last_date = v.strftime("%Y-%m-%d")
                else:  # pragma: no cover
                    next_date = datetime.strptime(v, "%Y-%m-%d") + timedelta(days=90)
                    last_date = v
                if next_date < today:
                    filtered_next_update[k] = {
                        "next_date": next_date.strftime("%Y-%m-%d"),
                        "last_date": last_date,
                    }

            self.logger.debug(f"{len(filtered_next_update)} tickers required.")
            return filtered_next_update
        else:  # pragma: no cover
            raise KeyError(f"{self.table} Table no found.")

    def get_required_tickers(self) -> list[str]:
        """Get valid tickers from SP500_BASIC_DETAILS."""

        is_successful, result = self.sqlite_client.query(
            "SELECT distinct(exchange_symbol) FROM  SP500_BASIC_DETAILS"
        )

        return [row[0] for row in result]

    def run(self):  # pragma: no cover
        """Run step."""

        required_tickers = self.get_required_tickers()
        last_update = self.get_last_update(required_tickers)

        self.output["required_tickers"] = last_update

        return True, self.output

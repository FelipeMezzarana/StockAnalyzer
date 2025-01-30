# Local
from ..clients.postgres_client import PostgresClient
from ..clients.sqlite_client import SQLiteClient
from ..common_steps.load_sql import SQLLoader
from ..common_steps.load_sql_many import SQLManyLoader
from ..common_steps.validate import Validator
from ..common_steps.validate_many import ManyValidator
from ..common_steps.wikipedia_extractor import WikipediaExtractor
from ..exceptions import StepNotFoundError
from ..pipelines.financials.steps.check_financials_tables import FinancialsChecker
from ..pipelines.financials.steps.extract_financials_data import FinancialsExtractor
from ..pipelines.stock_daily_prices.steps.extract_stock_daily_prices import StockDailyPriceExtractor
from ..pipelines.index_daily_close.steps.check_index_daily_close import IndexDailyCloseChecker
from ..pipelines.index_daily_close.steps.extract_index_daily_close import IndexDailyCloseExtractor
from ..pipelines.sp500_company_details.steps.check_sp500_company_details import SP500Checker
from ..pipelines.sp500_company_details.steps.transform_sp500_company_details import SP500Transformer
from ..pipelines.stock_company_details.steps.extract_stock_company_details import (
    TickerBasicDetailsExtractor,
)
from ..settings import Settings


class StepFactory:
    """Simple Processor Factory.
    Uses factory design pattern to create processors.
    """

    def __init__(self, settings: Settings):
        self.settings = settings

        self.clients = {
            "SQLITE": lambda settings: SQLiteClient(settings),
            "POSTGRES": lambda settings: PostgresClient(settings),
        }
        client = self.clients[self.settings.CLIENT](self.settings)

        self._steps = {
            "extract-stock-daily-prices": (
                lambda previous_output, settings, **kwargs: StockDailyPriceExtractor(
                    previous_output, settings, client, **kwargs
                )
            ),
            "extract-stock-company-details": (
                lambda previous_output, settings, **kwargss: TickerBasicDetailsExtractor(
                    previous_output, settings, client, **kwargss
                )
            ),
            "validate": lambda previous_output, settings, **kwargss: Validator(
                previous_output, settings, **kwargss
            ),
            "load-sql": lambda previous_output, settings, **kwargss: SQLLoader(
                previous_output, settings, client, **kwargss
            ),
            "extract-sp500-wiki-html": (
                lambda previous_output, settings, **kwargs: WikipediaExtractor(
                    "List_of_S&P_500_companies",
                    previous_output,
                    settings,
                    **kwargs,
                )
            ),
            "transform-sp500-table": lambda previous_output, settings, **kwargss: SP500Transformer(
                previous_output, settings, **kwargss
            ),
            "check-sp500-table": lambda previous_output, settings, **kwargss: SP500Checker(
                previous_output, settings, client, **kwargss
            ),
            "check-index-daily-close": (
                lambda previous_output, settings, **kwargss: IndexDailyCloseChecker(
                    previous_output, settings, client, **kwargss
                )
            ),
            "extract-index-daily-close": (
                lambda previous_output, settings, **kwargss: IndexDailyCloseExtractor(
                    previous_output, settings, **kwargss
                )
            ),
            "check-financials-tables": (
                lambda previous_output, settings, **kwargss: FinancialsChecker(
                    previous_output, settings, client, **kwargss
                )
            ),
            "extract-financials-data": (
                lambda previous_output, settings, **kwargss: FinancialsExtractor(
                    previous_output, settings, **kwargss
                )
            ),
            "validate-many": lambda previous_output, settings, **kwargss: ManyValidator(
                previous_output, settings, **kwargss
            ),
            "load-sql-many": lambda previous_output, settings, **kwargss: SQLManyLoader(
                previous_output, settings, client, **kwargss
            ),
        }

    def create(self, step, previous_output, **kwargs):
        """Returns instance of step if name is found.

        - step: name of the step.
        """

        if step not in self._steps:  # pragma: no cover
            raise StepNotFoundError(step, list(self._steps.keys()))

        return self._steps[step](previous_output, self.settings, **kwargs)

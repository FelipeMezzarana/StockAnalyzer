# Local
from ..common_steps.load_sqlite import SQLiteLoader
from ..common_steps.validate import Validator
from ..pipelines.grouped_daily.steps.extract_grouped_daily import GroupedDailyExtractor
from ..pipelines.ticker_basic_details.steps.extract_ticker_basic_details import (
    TickerBasicDetailsExtractor,
)
from ..settings import Settings
from ..common_steps.html_extractor import HtmlExtractor

class StepFactory:
    """Simple Processor Factory.
    Uses factory design pattern to create processors.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._steps = {
            "extract-grouped-daily-polygon": (
                lambda previous_output, settings, **kwargs: GroupedDailyExtractor(
                    previous_output, settings, **kwargs
                )
            ),
            "extract-ticker-basic-details-polygon": (
                lambda previous_output, settings, **kwargs: TickerBasicDetailsExtractor(
                    previous_output, settings, **kwargs
                )
            ),
            "validate": lambda previous_output, settings, **kwargs: Validator(
                previous_output, settings, **kwargs
            ),
            "load-sqlite": lambda previous_output, settings, **kwargs: SQLiteLoader(
                previous_output, settings, **kwargs
            ),
            "extract-sp500-wiki-html": lambda previous_output, settings, **kwargs: HtmlExtractor(
                "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",                
                previous_output, 
                settings, 
                **kwargs
            ),
        }

    def create(self, step, previous_output, **kwargs):
        """Returns instance of step if name is found.

        - step: name of the step.
        """

        if step not in self._steps:  # pragma: no cover
            raise ValueError(f"Step not found: {step}")

        return self._steps[step](previous_output, self.settings, **kwargs)

# Local
from ..exceptions import InvalidPipelineError
from ..pipelines.financials.financials_pipeline import FinancialsPipeline
from ..pipelines.grouped_daily.grouped_daily_pipeline import GroupedDailyPipeline
from ..pipelines.indexes_daily_close.indexes_daily_close_pipeline import IndexDailyClosePipeline
from ..pipelines.sp500_basic_details.sp500_basic_details_pipeline import SP500BasicDetailsPipeline
from ..pipelines.ticker_basic_details.ticker_basic_details_pipeline import (
    TickerBasicDetailsPipeline,
)
from ..settings import Settings
from ..utils.get_logger import get_logger


class PipelineFactory:
    """Pipeline Factory.
    Uses factory design pattern to create pipelines.
    """

    def __init__(self, settings: Settings):
        self.logger = get_logger(__name__, settings)
        self.pipeline = settings.pipeline
        self._pipelines = {
            "grouped-daily-pipeline": lambda: GroupedDailyPipeline(settings),
            "ticker-basic-details-pipeline": lambda: TickerBasicDetailsPipeline(settings),
            "sp500-basic-details-pipeline": lambda: SP500BasicDetailsPipeline(settings),
            "indexes-daily-close-pipeline": lambda: IndexDailyClosePipeline(settings),
            "financials-pipeline": lambda: FinancialsPipeline(settings),
        }

    def create(self):
        """Returns instance of pipeline if name is found.

        - pipeline: name of the pipeline.
        """

        if self.pipeline not in self._pipelines:  # pragma: no cover
            raise InvalidPipelineError(self.pipeline, self._pipelines.keys())

        # lazy initialization
        self.logger.info(f"Creating pipeline: {self.pipeline}")
        return self._pipelines[self.pipeline]()

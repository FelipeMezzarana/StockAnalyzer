# Local
from ..abstract.pipeline import Pipeline
from ..exceptions import InvalidPipelineError
from ..pipelines.financials.financials_pipeline import FinancialsPipeline
from ..pipelines.stock_daily_prices.stock_daily_prices_pipeline import StockDailyPricesPipeline
from ..pipelines.index_daily_close.index_daily_close_pipeline import IndexDailyClosePipeline
from ..pipelines.sp500_company_details.sp500_company_details_pipeline import SP500CompanyDetailsPipeline
from ..pipelines.stock_company_details.stock_company_details_pipeline import (
    StockCompanyDetailsPipeline,
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
            "stock-daily-prices-pipeline": lambda: StockDailyPricesPipeline(settings),
            "stock-company-details-pipeline": lambda: StockCompanyDetailsPipeline(settings),
            "sp500-company-details-pipeline": lambda: SP500CompanyDetailsPipeline(settings),
            "index-daily-close-pipeline": lambda: IndexDailyClosePipeline(settings),
            "financials-pipeline": lambda: FinancialsPipeline(settings),
        }

    def create(self) -> Pipeline:
        """Returns instance of pipeline if name is found.

        - pipeline: name of the pipeline.
        """

        if self.pipeline not in self._pipelines:  # pragma: no cover
            raise InvalidPipelineError(self.pipeline, tuple(self._pipelines.keys()))

        # lazy initialization
        self.logger.info(f"Creating pipeline: {self.pipeline}")
        return self._pipelines[self.pipeline]()

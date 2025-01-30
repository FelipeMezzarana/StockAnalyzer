# Local
from ...abstract.pipeline import Pipeline
from ...settings import Settings


class StockCompanyDetailsPipeline(Pipeline):
    """Update STOCK_COMPANY_DETAILS table.
    Get details from all companies of STOCK_DAILY_PRICES.
    """

    def __init__(self, settings: Settings) -> None:
        super(StockCompanyDetailsPipeline, self).__init__(__name__, settings)

    def build_steps(self):
        """Returns a list with valid steps specific to current pipeline."""

        # the order of the processors are important!
        return [
            "extract-stock-company-details",
            "validate",
            "load-sql",
        ]

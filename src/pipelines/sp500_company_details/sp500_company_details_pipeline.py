# Local
from ...abstract.pipeline import Pipeline
from ...settings import Settings


class SP500CompanyDetailsPipeline(Pipeline):
    """Update DB table SP500_COMPANY_DETAILS.
    Contain info about companies in S&P500 index.
    """

    def __init__(self, settings: Settings) -> None:
        super(SP500CompanyDetailsPipeline, self).__init__(__name__, settings)

    def build_steps(self):
        """Returns a list with valid steps specific to current pipeline."""

        # the order of the processors are important!
        return [
            "check-sp500-table",
            "extract-sp500-wiki-html",
            "transform-sp500-table",
            "validate",
            "load-sql",
        ]

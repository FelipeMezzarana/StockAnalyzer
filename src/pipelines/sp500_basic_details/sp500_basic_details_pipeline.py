# Local
from ...abstract.pipeline import Pipeline
from ...settings import Settings


class SP500BasicDeatailsPipeline(Pipeline):
    """Update DB table SP500_BASIC_DETAILS.
    Contain info about companys in S&P 500 index.
    """

    def __init__(self, settings: Settings) -> None:
        super(SP500BasicDeatailsPipeline, self).__init__(__name__, settings)

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

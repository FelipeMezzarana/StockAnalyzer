# Local
from ...abstract.pipeline import Pipeline
from ...settings import Settings


class FinancialsPipeline(Pipeline):
    """Update DB with."""

    def __init__(self, settings: Settings) -> None:
        super(FinancialsPipeline, self).__init__(__name__, settings)

    def build_steps(self):
        """Returns a list with valid steps specific to current pipeline."""

        # the order of the processors are important!
        return [
            "check-financials-tables",
            "extract-financials-data",
            "validate-many",
            "load-sql-many",
        ]

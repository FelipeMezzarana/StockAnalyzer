# Local
from ...abstract.pipeline import Pipeline
from ...settings import Settings


class TickerBasicDetailsPipeline(Pipeline):
    """Update DB with all missing dates for GROUPED_DAILY table.
    Get the daily open, high, low, and close (OHLC) for
    the entire stocks/equities markets.
    """

    def __init__(self, settings: Settings) -> None:
        super(TickerBasicDetailsPipeline, self).__init__(__name__, settings)

    def build_steps(self):
        """Returns a list with valid steps specific to current pipeline."""

        # the order of the processors are important!
        return [
            "extract-ticker-basic-details-polygon",
            "validate",
            "load-sql",
        ]

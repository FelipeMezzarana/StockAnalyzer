# Local
from ...abstract.pipeline import Pipeline
from ...settings import Settings


class TickerBasicDeatailsPipeline(Pipeline):
    """Update DB wirh all missind dates for GROUPED_DAILY table.
    Get the daily open, high, low, and close (OHLC) for
    the entire stocks/equities markets.
    """

    def __init__(self, settings: Settings) -> None:
        super(TickerBasicDeatailsPipeline, self).__init__(__name__, settings)

    def build_steps(self):
        """Returns a list with valid steps specific to current pipeline."""

        # the order of the processors are important!
        return [
            "extract-ticker-basic-details-polygon",
            "validate",
            "load-sqlite",
        ]
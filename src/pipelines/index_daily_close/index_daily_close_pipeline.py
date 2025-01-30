# Local
from ...abstract.pipeline import Pipeline
from ...settings import Settings


class IndexDailyClosePipeline(Pipeline):
    """Update DB table INDEX_DAILY_CLOSE.
    Contain close daily values for several indexes.
    """

    def __init__(self, settings: Settings) -> None:
        super(IndexDailyClosePipeline, self).__init__(__name__, settings)

    def build_steps(self):
        """Returns a list with valid steps specific to current pipeline."""

        # the order of the processors are important!
        return [
            "check-index-daily-close",
            "extract-index-daily-close",
            "validate",
            "load-sql",
        ]

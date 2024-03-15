
from ...abstract.pipeline import Pipeline

class GroupedDailyPipeline(Pipeline):

    def __init__(self) -> None:
        super(GroupedDailyPipeline, self).__init__(__name__)

    def build_steps(self):
        """Returns a list with valid steps specific to current pipeline."""

        # the order of the processors are important!
        return [
            "extract-grouped-daily-polygon",
        ]
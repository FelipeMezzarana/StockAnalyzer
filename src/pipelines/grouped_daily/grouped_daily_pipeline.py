
from ...abstract.pipeline import Pipeline
from ...settings import Settings
class GroupedDailyPipeline(Pipeline):

    def __init__(self, settings: Settings) -> None:
        super(GroupedDailyPipeline, self).__init__(__name__, settings)

    def build_steps(self):
        """Returns a list with valid steps specific to current pipeline."""

        # the order of the processors are important!
        return [
            "extract-grouped-daily-polygon",
        ]
# Local
from ..pipelines.grouped_daily.grouped_daily_pipeline import GroupedDailyPipeline
from ..settings import Settings


class PipelineFactory:
    """Pipeline Factory.
    Uses factory design pattern to create pipelines.
    """

    def __init__(self, settings: Settings):
        self.pipeline = settings.pipeline
        self._pipelines = {
            "grouped-daily-pipeline": lambda: GroupedDailyPipeline(settings),
        }

    def create(self):
        """Returns instance of pipeline if name is found.

        - pipeline: name of the pipeline.
        """

        if self.pipeline not in self._pipelines:
            raise ValueError(f"Pipeline not found: {self.pipeline}")

        # lazy initialization
        return self._pipelines[self.pipeline]()

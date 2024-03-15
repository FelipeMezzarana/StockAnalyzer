
from ..pipelines.grouped_daily.grouped_daily_pipeline import GroupedDailyPipeline



class PipelineFactory():
    """Pipeline Factory.
    Uses factory design pattern to create pipelines.
    """

    def __init__(self):
        self._pipelines = {
            "grouped-daily-pipeline": lambda: GroupedDailyPipeline(),
        }

    def create(self, pipeline):
        """Returns instance of pipeline if name is found.

        - pipeline: name of the pipeline.
        """

        if pipeline not in self._pipelines:
            raise ValueError(f"Pipeline not found: {pipeline}")

        # lazy initialization
        return self._pipelines[pipeline]()
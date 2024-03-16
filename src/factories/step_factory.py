
from ..pipelines.grouped_daily.steps.extract_grouped_daily import GroupedDailyExtractor
from ..settings import Settings


class StepFactory:
    """Simple Processor Factory.
    Uses factory design pattern to create processors.
    """

    def __init__(self, settings: Settings):

        self.settings = settings
        self._steps = {
            "extract-grouped-daily-polygon": 
            lambda previous_output, settings, **kwargs: GroupedDailyExtractor(previous_output, settings, **kwargs)
            }
    
    def create(self, step, previous_output, **kwargs):
        """Returns instance of step if name is found.

        - step: name of the step.
        """

        if step not in self._steps:
            raise ValueError(f"Step not found: {step}")

        return self._steps[step](previous_output, self.settings, **kwargs)
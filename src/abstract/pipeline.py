# Standard library
from abc import ABC, abstractmethod
from typing import List

# Local
from ..factories.step_factory import StepFactory
from ..settings import Settings
from ..utils.get_logger import get_logger


class Pipeline(ABC):
    """Abstract class used to run pipelines."""

    def __init__(self, name: str, settings: Settings, **kwargs):
        self.logger = get_logger(name, settings)
        self.name = settings.pipeline
        self.settings = settings
        self.kwargs = kwargs

    @abstractmethod
    def build_steps(self) -> List[str]:
        """Builds list of allowed steps in pipeline."""
        pass

    def run(self):
        """Summary: Executes step if it wasn't run before."""

        self.logger.info(f"Starting Pipeline: {self.name}")
        pipeline_steps = self.build_steps()
        output = None

        for step in pipeline_steps:
            self.logger.info(f"Starting {step=}")
            self.settings.step_name = step
            steps = StepFactory(self.settings)
            is_success, output = steps.create(step, output, **self.kwargs).run()
            self.logger.debug(f"{output=}")
            self.logger.info(f"Finished {step=} {is_success=}")
            if output.get("skip_pipeline"):
                self.logger.info(f"Step {step} marked pipeline to skip.")
                break

        self.logger.info(f"Finished Pipeline: {self.name}")
        return True

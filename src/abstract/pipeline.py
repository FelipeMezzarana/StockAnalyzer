# Standard library
import json
from abc import ABC, abstractmethod
from typing import List

# Local
from ..factories.step_factory import StepFactory
from ..settings import Settings
from ..util.get_logger import get_logger


class Pipeline(ABC):
    """Abstract class used to run pipelines."""

    def __init__(self, name: str, settings: Settings, **kwargs):
        self.logger = get_logger(__name__, settings)
        self.name = name
        self.kwargs = kwargs
        self.settings = settings

    @abstractmethod
    def build_steps(self) -> List[str]:
        """Builds list of allowed steps in pipeline."""
        pass

    def run(self):
        """Summary: Executes step if it wasn't run before."""

        pipeline_steps = self.build_steps()
        output = None

        for step in pipeline_steps:
            self.logger.info(f"starting {step=}")
            self.settings.step_name = step
            steps = StepFactory(self.settings)
            is_success, output = steps.create(step, output, **self.kwargs).run()
            self.logger.debug(f"{is_success=} -- {output=}")
            self.logger.info(f"finished {step=}")
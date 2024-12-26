# Standard library
from abc import ABC, abstractmethod

# Local
from ..settings import Settings
from ..utils.get_logger import get_logger


class Step(ABC):
    """Abstract class used to run steps of pipeline."""

    def __init__(self, name: str, previous_output: dict, settings: Settings):
        self.settings = settings
        self.previous_output = previous_output
        self.logger = get_logger(name, settings)
        self.logger = self.logger.bind(step=settings.step_name)
        self.output: dict = {}

    @abstractmethod
    def run(self):
        """Run the step."""
        pass

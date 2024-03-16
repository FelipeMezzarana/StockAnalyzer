# Standard library
from abc import ABC, abstractmethod

# Local
from ..util.get_logger import get_logger
from ..settings import Settings

class Step(ABC):
    """Abstract class used to run steps of pipeline."""

    def __init__(self,name: str,previous_output: dict, settings: Settings):

        self.settings = settings
        self.previous_output = previous_output
        self.logger = get_logger(name, settings)
        self.logger = self.logger.bind(step=settings.step_name)

    @abstractmethod
    def run(self):
        """Run the step."""
        pass
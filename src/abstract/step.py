# Standard library
from abc import ABC, abstractmethod

# Local
from ..util.get_logger import get_logger


class Step(ABC):
    """Abstract class used to run steps of pipeline."""

    def __init__(self):
        self.logger = get_logger(__name__)

    @abstractmethod
    def run(self):
        """Run the step."""
        pass
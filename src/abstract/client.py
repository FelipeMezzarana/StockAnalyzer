# Standard library
from abc import ABC, abstractmethod


class Client(ABC):
    """Abstract class used to create clients."""

    @abstractmethod
    def connect(self):
        """Connect to db."""
        pass

    @abstractmethod
    def execute(self, query: str):
        """Execute query."""
        pass

    @abstractmethod
    def executemany(self, query: str, mapped_values: list[tuple]):
        """Execute parameterized query."""
        pass

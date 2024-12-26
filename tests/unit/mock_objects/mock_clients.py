# Standard library
import logging


class MockSQLHandler:
    def __init__(self, query_results: list = [], *args, **kwargs):
        """Mock SQLHandler.

        Args:
            query_results: Results to be returned by self.query, in order.
        """
        self.logger = logging.getLogger(__name__)
        self.query_results = query_results
        self.query_index = 0

    def query(self, *args, **kwargs) -> tuple[bool, list]:
        """Mock query method."""

        if self.query_index > len(self.query_results):
            raise KeyError("self.query called but there is no more query results left.")
        self.query_index += 1
        self.logger.info(
            f"Mocking query result: {self.query_index} of {len(self.query_results)} results."
        )

        return True, self.query_results[self.query_index - 1]


class MockPolygon:
    """Mock Polygon client."""

    def __init__(self, response: dict = {}, *args, **kwargs):
        """Mock Polygon client."""
        self.response = response

    def request(self, url: str) -> dict:
        """Mock request method."""
        return self.response


class MockResponse:
    def __init__(self, text: str) -> None:
        self.status_code = 200
        self.text = text

    def json(self) -> dict:
        """Mock json method."""
        return {"parse": {"text": {"*": self.text}}}

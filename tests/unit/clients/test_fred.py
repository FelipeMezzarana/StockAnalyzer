# Standard library
import unittest
from unittest.mock import patch
import os

# First party
from src.clients.fred import Fred
from src.settings import Settings


class MockGetRequests:
    def __init__(self) -> None:
        self.status_code = 200

    def json(self):
        return True


class TestFred(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """ """
        cls.mock_settings = Settings("indexes-daily-close-pipeline")
        os.environ['FRED_KEY'] = "FRED_KEY"

    @patch("src.clients.fred.requests")
    def test_get_grouped_daily(self, mock_requests) -> None:
        """Test Polygon.get_grouped_daily()."""

        mock_requests.get.return_value = MockGetRequests()
        fred = Fred(self.mock_settings)

        result = fred.request("endpoint")
        self.assertTrue(result)

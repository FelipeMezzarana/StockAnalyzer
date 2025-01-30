# Standard library
import os
import unittest
from unittest.mock import patch

# First party
from src.clients.polygon import Polygon
from src.settings import Settings


class MockGetRequests:
    def __init__(self) -> None:
        self.status_code = 200

    def json(self):
        return True


class TestPolygon(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """ """
        cls.mock_settings = Settings("stock-daily-prices-pipeline")
        os.environ["POLYGON_KEY"] = "POLYGON_KEY"

    @patch("src.clients.polygon.requests")
    def test_get_stock_daily_prices(self, mock_requests) -> None:
        """Test Polygon.get_stock_daily_prices()."""

        mock_requests.get.return_value = MockGetRequests()
        polygon = Polygon(self.mock_settings)
        for i in range(2):  # test api_limit
            result = polygon.request("endpoint")
        self.assertTrue(result)

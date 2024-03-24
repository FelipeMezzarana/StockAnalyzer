# Standard library
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
        cls.mock_settings = Settings("mock_pipeline")

    @patch("src.clients.polygon.requests")
    def test_get_grouped_daily(self, mock_requests) -> None:
        """Test Polygon.get_grouped_daily()."""

        mock_requests.get.return_value = MockGetRequests()
        polygon = Polygon(self.mock_settings)
        for i in range(2): # test api_limit
            result = polygon.get_grouped_daily("2024-01-01")
        self.assertTrue(result)

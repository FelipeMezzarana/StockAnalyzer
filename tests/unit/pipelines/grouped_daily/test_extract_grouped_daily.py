# Standard library
import json
import os
import unittest
from unittest.mock import patch

# First party
from src.clients.sqlite_client import SQLiteClient
from src.pipelines.stock_daily_prices.steps.extract_stock_daily_prices import (
    StockDailyPriceExtractor,
)
from src.settings import Settings

SAMPLE_REQUEST_FILE = "tests/unit/data_samples/request_stock_daily_prices_sample.json"


class TestExtractGroupedDaily(unittest.TestCase):
    """Test StockDailyPriceExtractor."""

    @classmethod
    def setUpClass(cls):
        """Class Setup."""
        cls.settings = Settings("stock-daily-prices-pipeline")
        cls.settings.CLIENT_CONFIG["DB_PATH"] = "database/mock_stock_database.db"
        cls.client = SQLiteClient(cls.settings)
        cls.settings.POLYGON["POLYGON_MAX_DAYS_HIST"] = 10

    @patch("src.pipelines.stock_daily_prices.steps.extract_stock_daily_prices.Polygon")
    def test_run(self, mock_polygon) -> None:
        """Test create pipeline."""

        # Mock API result
        with open(SAMPLE_REQUEST_FILE, "r") as file:
            sample_request_result = json.load(file)
        mock_polygon.return_value.request.return_value = sample_request_result

        # Extract
        extractor = StockDailyPriceExtractor({}, self.settings, self.client)
        is_successful, output = extractor.run()
        self.assertTrue(is_successful)

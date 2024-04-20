# Standard library
import json
import unittest
from unittest.mock import patch

# First party
from src.pipelines.grouped_daily.steps.extract_grouped_daily import GroupedDailyExtractor
from src.settings import Settings

SAMPLE_REQUEST_FILE = "tests/unit/data_samples/request_grouped_daily_sample.json"


class TestExtractGroupedDaily(unittest.TestCase):
    """Test GroupedDailyExtractor."""

    @classmethod
    def setUpClass(cls):
        """Class Setup."""
        cls.settings = Settings("grouped-daily-pipeline")
        cls.settings.DB_PATH = "src/database/mock_stock_database.db"
        cls.settings.POLYGON_MAX_DAYS_HIST = 10

    @patch("src.pipelines.grouped_daily.steps.extract_grouped_daily.Polygon")
    def test_run(self, mock_polygon) -> None:
        """Test create pipeline."""

        # Mock API result
        with open(SAMPLE_REQUEST_FILE, "r") as file:
            sample_request_result = json.load(file)
        mock_polygon.return_value.get_grouped_daily.return_value = sample_request_result

        # Extract
        extractor = GroupedDailyExtractor({}, self.settings)
        is_successful, output = extractor.run()
        self.assertTrue(is_successful)

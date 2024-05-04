# Standard library
import json
import unittest
from unittest.mock import patch

# First party
from src.pipelines.ticker_basic_details.steps.extract_ticker_basic_details import (
    TickerBasicDetailsExtractor,
)
from src.settings import Settings

SAMPLE_REQUEST_FILE = "tests/unit/data_samples/request_ticker_basic_details_sample.json"


class TestExtractTickerDetails(unittest.TestCase):
    """Test TickerBasicDetailsExtractor."""

    @classmethod
    def setUpClass(cls):
        """Class Setup."""
        cls.settings = Settings("ticker-basic-details-pipeline")
        cls.settings.DB_PATH = "src/database/mock_stock_database.db"
        cls.settings.POLYGON_MAX_DAYS_HIST = 10

    @patch(
        "src.pipelines.ticker_basic_details.steps.extract_ticker_basic_details."
        "TickerBasicDetailsExtractor.get_required_tickers"
    )
    @patch("src.pipelines.ticker_basic_details.steps.extract_ticker_basic_details.Polygon")
    def test_run(self, mock_polygon, mock_tickers) -> None:
        """Test create pipeline."""

        # Mock API result
        with open(SAMPLE_REQUEST_FILE, "r") as file:
            sample_request_result = json.load(file)
        mock_polygon.return_value.request.return_value = sample_request_result
        mock_tickers.return_value = [r["ticker"] for r in sample_request_result["results"]]

        # Extract
        extractor = TickerBasicDetailsExtractor({}, self.settings)
        is_successful, output = extractor.run()
        self.assertTrue(is_successful)

    @patch("src.pipelines.ticker_basic_details.steps.extract_ticker_basic_details.SQLiteHandler")
    def test_get_required_tickers(self, mock_sql):
        """ """
        mock_sql.return_value.query.return_value = True, ["ticker1", "ticker2"]
        extractor = TickerBasicDetailsExtractor({}, self.settings)
        required_tickers = extractor.get_required_tickers()
        self.assertEqual(required_tickers, [])

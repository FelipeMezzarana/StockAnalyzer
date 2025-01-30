# Standard library
import json
import unittest
from unittest.mock import patch

# First party
from src.clients.sqlite_client import SQLiteClient
from src.pipelines.stock_company_details.steps.extract_stock_company_details import (
    TickerBasicDetailsExtractor,
)
from src.settings import Settings

SAMPLE_REQUEST_FILE = "tests/unit/data_samples/request_stock_company_details_sample.json"


class TestExtractTickerDetails(unittest.TestCase):
    """Test TickerBasicDetailsExtractor."""

    @classmethod
    def setUpClass(cls):
        """Class Setup."""
        cls.settings = Settings("stock-company-details-pipeline")
        cls.settings.CLIENT_CONFIG["DB_PATH"] = "database/mock_stock_database.db"
        cls.client = SQLiteClient(cls.settings)
        cls.settings.POLYGON["POLYGON_MAX_DAYS_HIST"] = 10

    @patch(
        "src.pipelines.stock_company_details.steps.extract_stock_company_details."
        "TickerBasicDetailsExtractor.get_required_tickers"
    )
    @patch("src.pipelines.stock_company_details.steps.extract_stock_company_details.Polygon")
    def test_run(self, mock_polygon, mock_tickers) -> None:
        """Test create pipeline."""

        # Mock API result
        with open(SAMPLE_REQUEST_FILE, "r") as file:
            sample_request_result = json.load(file)
        mock_polygon.return_value.request.return_value = sample_request_result
        mock_tickers.return_value = (
            [r["ticker"] for r in sample_request_result["results"]],
            [r["ticker"] for r in sample_request_result["results"][:5]],
        )

        # Extract
        extractor = TickerBasicDetailsExtractor({}, self.settings, self.client)
        is_successful, _ = extractor.run()
        self.assertTrue(is_successful)

    @patch("src.pipelines.stock_company_details.steps.extract_stock_company_details.SQLHandler")
    def test_get_required_tickers(self, mock_sql):
        """ """
        mock_sql.return_value.query.return_value = True, ["ticker1", "ticker2"]
        extractor = TickerBasicDetailsExtractor({}, self.settings, self.client)
        required_tickers, _ = extractor.get_required_tickers()
        self.assertEqual(required_tickers, [])

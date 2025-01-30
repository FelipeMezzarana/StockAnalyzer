# Standard library
import unittest
from unittest.mock import patch

# First party
from src.common_steps.wikipedia_extractor import WikipediaExtractor
from src.settings import Settings
from tests.unit.mock_objects.mock_clients import MockResponse

SAMPLE_HTML_FILE = "tests/unit/data_samples/sp500_company_details_sample.txt"


class TestWikipediaExtractor(unittest.TestCase):
    """Test WikipediaExtractor class."""

    @classmethod
    def setUpClass(cls):
        """Class Setup."""
        cls.settings = Settings("sp500-company-details-pipeline")

    @patch(
        "src.common_steps.wikipedia_extractor.requests",
    )
    def test_run(self, mock_requests) -> None:
        """Test run html extractor."""

        with open(SAMPLE_HTML_FILE, "r") as f:
            expected_html = f.read()
        mock_requests.get.return_value = MockResponse(expected_html)

        html_extractor = WikipediaExtractor("webpage", {}, self.settings)
        is_successful, output = html_extractor.run()
        self.assertTrue(is_successful)

        with open(output["file_path"], "r") as f:
            actual_html = f.read()
        self.assertEqual(expected_html, actual_html)

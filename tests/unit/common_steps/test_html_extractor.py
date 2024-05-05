# Standard library
import unittest
from unittest.mock import patch

# First party
from src.common_steps.html_extractor import HtmlExtractor
from src.settings import Settings

SAMPLE_HTML_FILE = "tests/unit/data_samples/sp500_basic_details_sample.txt"

class MockResponse:

    def __init__(self, text: str) -> None:
        self.status_code = 200
        self.text = text


class TestHtmlExtractor(unittest.TestCase):
    """Test HtmlExtractor class."""

    @classmethod
    def setUpClass(cls):
        """Class Setup."""
        cls.settings = Settings("sp500-basic-details-pipeline")
        

    @patch("src.common_steps.html_extractor.requests", )
    def test_run(self, mock_requests) -> None:
        """Test run html extractor."""

        with open(SAMPLE_HTML_FILE, 'r') as f:
            expected_html = f.read()
        mock_requests.get.return_value = MockResponse(expected_html)

        html_extractor = HtmlExtractor("webpage", {}, self.settings)
        is_successful, output = html_extractor.run()
        self.assertTrue(is_successful)

        with open(output["file_path"], 'r') as f:
            actual_html = f.read()
        self.assertEqual(expected_html, actual_html)

# Standard library
import unittest
from unittest.mock import patch
from datetime import datetime
# First party
from src.pipelines.sp500_basic_details.steps.check_sp500_basic_details import SP500Checker
from src.settings import Settings

SAMPLE_HTML_FILE = "tests/unit/data_samples/sp500_basic_details_sample.txt"
EXPECTED_RESULT = "tests/unit/data_samples/sp500_basic_details.csv"


class TestSP500Transformer(unittest.TestCase):
    """Test SP500Transformer class."""

    @classmethod
    def setUpClass(cls):
        """Class Setup."""
        cls.settings = Settings("sp500-basic-details-pipeline")
        cls.previous_output = {"file_path": SAMPLE_HTML_FILE}

    @patch("src.pipelines.sp500_basic_details.steps.check_sp500_basic_details.SQLiteHandler")
    def test_run(self, mock_sqlite) -> None:
        """Test run."""

        mock_sqlite.return_value.query.side_effect = [
            (True, [(datetime.today().strftime("%Y-%m-%d %H:%M:%S"), )]), # Skip
            (True, [(datetime(1990,1,1).strftime("%Y-%m-%d %H:%M:%S"), )]) # Run
        ]
        sp500_checker = SP500Checker({}, self.settings)

        # Check skip
        is_successful, output = sp500_checker.run()
        self.assertTrue(output["skip_pipeline"])

        # Check run
        is_successful, output = sp500_checker.run()
        self.assertFalse(output["skip_pipeline"])
     

        
   

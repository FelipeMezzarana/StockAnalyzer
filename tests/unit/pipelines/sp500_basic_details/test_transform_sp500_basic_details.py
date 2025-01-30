# Standard library
import os
import unittest

# Third party
import duckdb

# First party
from src.pipelines.sp500_company_details.steps.transform_sp500_company_details import (
    SP500Transformer,
)
from src.settings import Settings

SAMPLE_HTML_FILE = "tests/unit/data_samples/sp500_company_details_sample.txt"
EXPECTED_RESULT = "tests/unit/data_samples/sp500_company_details.csv"


class TestSP500Transformer(unittest.TestCase):
    """Test SP500Transformer class."""

    @classmethod
    def setUpClass(cls):
        """Class Setup."""
        cls.settings = Settings("sp500-company-details-pipeline")
        cls.previous_output = {"file_path": SAMPLE_HTML_FILE}

    def test_run(self) -> None:
        """Test run."""

        sp500_transformer = SP500Transformer(self.previous_output, self.settings)
        is_successful, output = sp500_transformer.run(clean_file=False)
        self.assertTrue(is_successful)
        self.assertTrue(os.path.isfile(output["file_path"]))

        actual_header = duckdb.read_csv(output["file_path"], header=False).fetchone()
        expected_header = duckdb.read_csv(EXPECTED_RESULT, header=False).fetchone()
        self.assertEqual(actual_header, expected_header)

        actual_data = duckdb.read_csv(output["file_path"], header=True).fetchall()
        expected_data = duckdb.read_csv(EXPECTED_RESULT, header=True).fetchall()
        self.assertEqual(len(actual_data), len(expected_data))

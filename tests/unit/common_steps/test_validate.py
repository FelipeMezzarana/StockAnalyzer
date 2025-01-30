# Standard library
import os
import unittest

# Third party
import duckdb

# First party
from src.common_steps.validate import Validator
from src.settings import Settings

SAMPLE_VALID_FILE = "tests/unit/data_samples/stock_daily_prices_sample.csv"
SAMPLE_INVALID_FILE = "tests/unit/data_samples/invalid_stock_daily_prices_sample.csv"


class TestValidator(unittest.TestCase):
    """Test Validator class."""

    @classmethod
    def setUpClass(cls):
        """Class Setup."""
        cls.settings = Settings("stock-daily-prices-pipeline")

    def test_run_valid(self) -> None:
        """Test run validator with valid data only."""

        previous_output = {"file_path": SAMPLE_VALID_FILE}
        validator = Validator(previous_output, self.settings)
        validator.run()

        valid_file_path = validator.output["valid_file_path"]
        invalid_file_path = validator.output["invalid_file_path"]

        self.assertTrue(os.path.isfile(valid_file_path))
        self.assertTrue(os.path.isfile(invalid_file_path))
        self.assertFalse(os.path.isfile(previous_output["file_path"]))

        raw_file = duckdb.read_csv(invalid_file_path, header=True).fetchall()
        self.assertEqual(len(raw_file), 0)

    def test_run_invalid(self) -> None:
        """Test run validator with invalid data."""

        previous_output = {"file_path": SAMPLE_INVALID_FILE}
        validator = Validator(previous_output, self.settings)
        validator.run()

        valid_file_path = validator.output["valid_file_path"]
        invalid_file_path = validator.output["invalid_file_path"]

        self.assertTrue(os.path.isfile(valid_file_path))
        self.assertTrue(os.path.isfile(invalid_file_path))
        self.assertFalse(os.path.isfile(previous_output["file_path"]))

        raw_file = duckdb.read_csv(invalid_file_path, header=True).fetchall()
        self.assertEqual(len(raw_file), 3)

# Standard library
import unittest
import json
import os

# First party
from src.util import csv_handler

SAMPLE_REQUEST_FILE = "tests/unit/data_samples/request_grouped_daily_sample.json"

class TestCSVHandler(unittest.TestCase):
    """Test PipelineFactory."""
    @classmethod
    def setUpClass(cls):
        """ """
        
        cls.temp_file = "test_csv_handler.csv"
        with open(SAMPLE_REQUEST_FILE, 'r') as file:
            cls.sample_request_result = json.load(file)


    def test_csv_handler(self) -> None:
        """Test create pipeline."""

        # Create csv
        csv_handler.append_to_file(
            self.temp_file, 
            self.sample_request_result["results"]
            )
        self.assertTrue(os.path.isfile(self.temp_file))
        # Delete csv
        csv_handler.clean_temp_file(self.temp_file)
        self.assertFalse(os.path.isfile(self.temp_file))

        

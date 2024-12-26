# Standard library
import csv
import json
import unittest
from unittest.mock import patch

# First party
from src.pipelines.indexes_daily_close.steps.extract_index_daily_close import (
    IndexDailyCloseExtractor,
)
from src.settings import Settings

SAMPLE_REQUEST_FILE = "tests/unit/data_samples/extract_index_daily_close_sample.json"


class TestIndexDailyCloseExtractor(unittest.TestCase):
    """Test IndexDailyCloseExtractor."""

    @classmethod
    def setUpClass(cls):
        """Class Setup."""
        cls.settings = Settings("indexes-daily-close-pipeline")
        cls.settings.FRED["INDEXES"] = ["SP500"]  # Limit test to one prefix

    @patch("src.pipelines.indexes_daily_close.steps.extract_index_daily_close.Fred")
    def test_run(self, mock_fred) -> None:
        """Test create pipeline."""

        # Mock API result
        with open(SAMPLE_REQUEST_FILE, "r") as file:
            sample_request_result = json.load(file)
        mock_fred.return_value.request.return_value = sample_request_result

        # Extract
        previous_output = {"indexes_last_update": {"SP500": "1600-01-01"}}
        extractor = IndexDailyCloseExtractor(previous_output, self.settings)
        is_successful, output = extractor.run()
        self.assertTrue(is_successful)

        # Open output csv file
        file_name = output["file_path"]
        with open(file_name, mode="r") as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader)
            rows = []
            for row in csv_reader:
                rows.append(row)

            data = {k: [r[i] for r in rows] for k, i in zip(header, range(len(header)))}

        index_code = data["index"][0]
        total_value = sum([round(float(v), 0) for v in data["value"] if v])
        self.assertEqual(index_code, "SP500")
        self.assertEqual(total_value, 42701)

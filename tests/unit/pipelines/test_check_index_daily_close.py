# Standard library
import unittest
from datetime import datetime
from unittest.mock import patch

# First party
from src.pipelines.indexes_daily_close.steps.check_index_daily_close import IndexDailyCloseChecker
from src.settings import Settings


class TestIndexDailyCloseChecker(unittest.TestCase):
    """Test IndexDailyCloseChecker class."""

    @classmethod
    def setUpClass(cls):
        """Class Setup."""
        cls.settings = Settings("indexes-daily-close-pipeline")
        cls.indexes = cls.settings.FRED["INDEXES"]

    @patch("src.pipelines.indexes_daily_close.steps.check_index_daily_close.SQLiteHandler")
    def test_run(self, mock_sqlite) -> None:
        """Test run."""

        query_result_skip = [
            (index, datetime.today().strftime("%Y-%m-%d")) for index in self.indexes
        ]
        mock_sqlite.return_value.query.side_effect = [
            (True, query_result_skip),  # Skip, all indexes updated
            (True, [("SP500", "2024-06-27")]),  # Run, missing indexes
            (True, []),  # Run
        ]

        index_checker = IndexDailyCloseChecker({}, self.settings)

        # Check skip
        _, output = index_checker.run()
        self.assertTrue(output["skip_pipeline"])

        # Check run
        for n in range(2):
            _, output = index_checker.run()
            self.assertFalse(output["skip_pipeline"])

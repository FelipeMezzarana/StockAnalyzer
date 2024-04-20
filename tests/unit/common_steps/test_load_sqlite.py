# Standard library
import unittest

# First party
from src.common_steps.load_sqlite import SQLiteLoader
from src.settings import Settings


class TestLoadSQLite(unittest.TestCase):
    """Test PipelineFactory."""

    @classmethod
    def setUpClass(cls):
        """Class Setup."""
        cls.settings = Settings("grouped-daily-pipeline")
        cls.settings.DB_PATH = "src/database/stock_database_test.db"

        cls.sqlite_client = SQLiteLoader(
            {"valid_file_path": "tests/unit/data_samples/grouped_daily_sample.csv"}, cls.settings
        )

    def test_run(self) -> None:
        """Test create pipeline."""

        is_successful, output = self.sqlite_client.run(clean_file=False)
        self.assertTrue(is_successful)

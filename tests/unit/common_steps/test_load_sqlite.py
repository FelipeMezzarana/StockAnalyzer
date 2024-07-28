# Standard library
import unittest

# First party
from src.clients.sqlite_client import SQLiteClient
from src.common_steps.load_sql import SQLLoader
from src.settings import Settings


class TestLoadSQLite(unittest.TestCase):
    """Test PipelineFactory."""

    @classmethod
    def setUpClass(cls):
        """Class Setup."""
        cls.settings = Settings("grouped-daily-pipeline")
        cls.settings.CLIENT_CONFIG["DB_PATH"] = "database/stock_database_test.db"
        cls.client = SQLiteClient(cls.settings)
        cls.sqlite_client = SQLLoader(
            {
                "valid_file_path": "tests/unit/data_samples/grouped_daily_sample.csv",
                "invalid_file_path": "tests/unit/data_samples/invalid_grouped_daily_sample.csv",
            },
            cls.settings,
            cls.client,
        )

    def test_run(self) -> None:
        """Test create pipeline."""

        is_successful, output = self.sqlite_client.run(clean_file=False)
        self.assertTrue(is_successful)

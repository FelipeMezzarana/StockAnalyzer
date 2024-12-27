# Standard library
import os
import unittest

# Third party
import duckdb

# First party
from src.clients.sqlite_client import SQLiteClient
from src.settings import Settings
from src.utils.sql_handler import SQLHandler

SAMPLE_FILE = "tests/unit/data_samples/grouped_daily_sample.csv"


class TestSQLHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """ """
        cls.settings = Settings("grouped-daily-pipeline")
        cls.settings.CLIENT_CONFIG["DB_PATH"] = "stock_database.db"
        cls.client = SQLiteClient(cls.settings)
        cls.sqlite_handler = SQLHandler(cls.settings, cls.client)

    @classmethod
    def setDownClass(cls):
        """Clean test db"""

        if os.path.isfile(cls.settings.CLIENT_CONFIG["DB_PATH"]):
            os.remove(cls.settings.CLIENT_CONFIG["DB_PATH"])

    def test_create_table(self) -> None:
        """Test SQLHandler.create_table() and initialization."""

        # Assert DB Creation
        self.assertTrue(os.path.isfile(self.settings.CLIENT_CONFIG["DB_PATH"]))
        # Create table based on settings
        self.sqlite_handler.create_table()
        is_successful, result = self.sqlite_handler.query(
            "SELECT * FROM BRONZE_LAYER.GROUPED_DAILY"
        )
        self.assertTrue(is_successful)

    def test_insert_into(self):
        """Test SQLHandler.insert_into()."""

        raw_file = duckdb.read_csv(SAMPLE_FILE, header=True).fetchall()
        header = duckdb.read_csv(SAMPLE_FILE, header=False).fetchone()

        self.sqlite_handler.insert_into(raw_file, header)
        is_successful, result = self.sqlite_handler.query(
            "SELECT count(*) FROM BRONZE_LAYER.GROUPED_DAILY"
        )
        self.assertEqual(result[0][0], 32)

# Standard library
import unittest
import os
import duckdb

# First party
from src.clients.sqlite_handler import SQLiteHandler
from src.settings import Settings

SAMPLE_FILE = "tests/unit/data_samples/grouped_daily_sample.csv"

class TestSQLiteHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """ """
        cls.settings = Settings("grouped-daily-pipeline")
        cls.settings.DB_PATH = "stock_database.db"
        cls.sqlite_handler = SQLiteHandler(cls.settings)

    @classmethod
    def setDownClass(cls):
        """Clean test db"""

        if os.path.isfile(cls.settings.DB_PATH):
            os.remove(cls.settings.DB_PATH)

    def test_create_table(self) -> None:
        """Test SQLiteHandler.create_table() and initialization."""

        # Assert DB Creation
        self.assertTrue(os.path.isfile(self.settings.DB_PATH))
        # Create table based on settings 
        self.sqlite_handler.create_table()
        is_successful, result = self.sqlite_handler.query(
            "SELECT * FROM GROUPED_DAILY"
            )
        self.assertTrue(is_successful)

    def test_insert_into(self):
        """Test SQLiteHandler.insert_into()."""

        raw_file = duckdb.read_csv(SAMPLE_FILE, header = True).fetchall()
        header = duckdb.read_csv(SAMPLE_FILE, header = False).fetchone()
        
        self.sqlite_handler.insert_into(raw_file, header)
        is_successful, result = self.sqlite_handler.query(
            "SELECT count(*) FROM GROUPED_DAILY"
            )
        self.assertEqual(result[0][0], 32)
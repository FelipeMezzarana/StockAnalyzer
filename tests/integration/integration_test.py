# Standard library
import os
import unittest

# First party
from src.clients.sqlite_client import SQLiteClient
from src.factories.pipeline_factory import PipelineFactory
from src.settings import PIPELINES, Settings
from src.util.sql_handler import SQLHandler


class TestIntegration(unittest.TestCase):
    """Test PipelineFactory."""

    def test_run(self) -> None:
        """Run pipelines.
        Mock few settings to load less data.
        """
        os.environ["CLIENT"] = "SQLITE"
        for pipeline in PIPELINES:
            settings = Settings(pipeline)
            # Create new DB for tests
            settings.CLIENT_CONFIG["DB_PATH"] = "database/stock_database_test.db"
            client = SQLiteClient(settings)
            # Limit data extracted
            settings.POLYGON["MAX_PAGINATION"] = 2
            settings.POLYGON["POLYGON_MAX_DAYS_HIST"] = 4
            settings.FRED["INDEXES"] = ["SP500"]

            # Run Pipeline
            pipeline = PipelineFactory(settings).create()
            is_successful = pipeline.run()
            self.assertTrue(is_successful)
            # Check db
            sqlite_handler = SQLHandler(settings, client)
            table_name = settings.PIPELINE_TABLE["name"]
            is_successful, rows_qty = sqlite_handler.query(f"SELECT COUNT(*) FROM {table_name}")
            self.assertTrue(is_successful)
            self.assertGreater(rows_qty[0][0], 10)  # Check if we have some data

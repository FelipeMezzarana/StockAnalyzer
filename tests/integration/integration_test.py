# Standard library
import unittest

# First party
from src.clients.sqlite_handler import SQLiteHandler
from src.factories.pipeline_factory import PipelineFactory
from src.settings import PIPELINES, Settings


class TestIntegration(unittest.TestCase):
    """Test PipelineFactory."""

    def test_run(self) -> None:
        """Run pipelines.
        Mock few settings to load less data.
        """

        for pipeline in PIPELINES:
            settings = Settings(pipeline)
            # Create new DB for tests
            settings.DB_PATH = "database/stock_database_test.db"
            # Limit data extracted
            settings.MAX_PAGINATION = 2
            settings.POLYGON_MAX_DAYS_HIST = 4

            # Run Pipeline
            pipeline = PipelineFactory(settings).create()
            is_successful = pipeline.run()
            self.assertTrue(is_successful)
            # Check db
            sqlite_handler = SQLiteHandler(settings)
            table_name = settings.PIPELINE_TABLE["name"]
            is_successful, rows_qty = sqlite_handler.query(f"SELECT COUNT(*) FROM {table_name}")
            self.assertTrue(is_successful)
            self.assertGreater(rows_qty[0][0], 10)  # Check if we have some data

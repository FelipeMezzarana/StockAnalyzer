# Standard library
import unittest

# First party
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
            # Alter settings for tests
            settings.DB_PATH = "src/database/stock_database_test.db"
            settings.POLYGON_MAX_DAYS_HIST = 5
            # Run Pipeline
            pipeline = PipelineFactory(settings).create()
            is_successful = pipeline.run()
            self.assertTrue(is_successful)

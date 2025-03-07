# Standard library
import unittest

# First party
from src.factories.pipeline_factory import PipelineFactory
from src.settings import PIPELINES, Settings


class TestPipelineFactory(unittest.TestCase):
    """Test PipelineFactory."""

    def test_create(self) -> None:
        """Test create pipeline."""

        for pipeline in PIPELINES.get_all_pipelines():
            settings = Settings(pipeline)
            pipeline_obj = PipelineFactory(settings).create()
            self.assertIsNotNone(pipeline_obj)

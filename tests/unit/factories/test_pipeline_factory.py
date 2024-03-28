# Standard library
import unittest

# First party
from src.factories.pipeline_factory import PipelineFactory
from src.settings import Settings, PIPELINES


class TestPipelineFactory(unittest.TestCase):
    """Test PipelineFactory."""

    def test_create(self) -> None:
        """Test create pipeline."""

        for pipeline in PIPELINES:
            settings = Settings(pipeline)
            pipeline_obj = PipelineFactory(settings).create()
            self.assertIsNotNone(pipeline_obj)

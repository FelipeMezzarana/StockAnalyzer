# Standard library
import unittest

# First party
from src.factories.pipeline_factory import PipelineFactory
from src.factories.step_factory import StepFactory
from src.settings import PIPELINES, Settings


class TestStepFactory(unittest.TestCase):
    """Test PipelineFactory."""

    def test_create(self) -> None:
        """Test create pipeline."""

        mock_output = {
            "file_path": "path",
            "valid_file_path": "path",
            "invalid_file_path": "path"
            }

        for pipeline in PIPELINES:
            settings = Settings(pipeline)
            pipeline_obj = PipelineFactory(settings).create()
            steps = pipeline_obj.build_steps()
            for step in steps:
                step_obj = StepFactory(settings).create(step, mock_output)
                self.assertIsNotNone(step_obj)

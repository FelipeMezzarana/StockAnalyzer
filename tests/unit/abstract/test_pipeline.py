import unittest
from unittest.mock import Mock, patch
from src.abstract.pipeline import Pipeline
from src.settings import Settings

class TestPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        """
        pass
        
    @patch("src.factories.step_factory.StepFactory")
    def test_pipeline(self,mock_step_factory)-> None:
        """Test Pipiline abstract class.
        """

        mock_settings = Settings("mock_pipeline")
        pipeline = Pipeline("mock_pipeline",mock_settings)

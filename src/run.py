# Local
from .factories.pipeline_factory import PipelineFactory
from .settings import Settings
from .utils.constants import PIPELINES


def run():
    """Run app."""
    for pipeline in PIPELINES:
        settings = Settings(pipeline)
        pipeline_factory = PipelineFactory(settings)
        pipeline = pipeline_factory.create()
        pipeline.run()


if __name__ == "__main__":
    run()

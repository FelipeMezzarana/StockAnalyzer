from . import settings
from .factories.pipeline_factory import PipelineFactory

def run():


    pipeline_factory = PipelineFactory()
    for pipeline in settings.PIPELINES:
        pipeline = pipeline_factory.create(pipeline)
        pipeline.run()


if __name__ == "__main__":
    run()
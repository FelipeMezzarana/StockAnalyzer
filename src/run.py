from .settings import Settings
from .factories.pipeline_factory import PipelineFactory


# Pipelines to run, in order.
PIPELINES = [
    "grouped-daily-pipeline"
]


def run():
    
    for pipeline in PIPELINES:
        settings = Settings(pipeline)
        pipeline_factory = PipelineFactory(settings)
        pipeline = pipeline_factory.create()
        pipeline.run()

if __name__ == "__main__":
    run()
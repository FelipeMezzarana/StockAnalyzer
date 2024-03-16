
import logging
from ..settings import Settings
import structlog

def get_logger(name: str, settings: Settings):
    """Return configured logger."""

    logger = structlog.get_logger(name)
    # the logger is only created "when it's needed". Enforce that:
    logger.new()
    structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(settings.LOGGING_LEVEL),
        )
    logger= logger.bind(__name__=name)
    logger= logger.bind(pipeline=settings.pipeline)

    return logger   


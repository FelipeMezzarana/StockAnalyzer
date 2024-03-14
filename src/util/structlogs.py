
import logging
from .. import settings

def get_structlogger(name: str):
    """Return configured logger."""

    logging.basicConfig(
        format='%(levelname)s - Line %(lineno)d (%(name)s %(asctime)s) - %(message)s',
        datefmt= '%Y-%m-%d %H:%M', 
        level=settings.LOGGING_LEVEL
        )
    
    logger = logging.getLogger(name)
    return logger    

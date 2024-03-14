import sqlite3
from .. import settings 
import os
from ..util import structlogs


class SQLiteHandler:
    """SQLLite db operations handler.
    """
    def __init__(self):

        self.logger = structlogs.get_structlogger(__name__)

        # Create db file if not exist
        self.logger.debug(f"{settings.DB_PATH=}")
        if settings.DB_PATH not in os.listdir():
            self.logger.critical
            (f"Database file {self.db_path} not fount. A new file will be created"
             )
        
        self.conn = sqlite3.connect(settings.DB_PATH)
        self.logger.info("conected")


        

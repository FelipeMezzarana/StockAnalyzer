import sqlite3
from .. import settings 
import os
from ..util.get_logger import get_logger


class SQLiteHandler:
    """SQLLite db operations handler.
    """
    def __init__(self):

        self.logger = get_logger(__name__)

        # Create db file if not exist
        self.logger.debug(f"{settings.DB_PATH=}")
        if not os.path.exists(settings.DB_PATH):
            self.logger.critical
            (f"Database file {settings.DB_PATH} not fount. A new file will be created"
             )
        
        self.conn = sqlite3.connect(settings.DB_PATH)
        self.cur = self.conn.cursor()
        self.logger.info("conected")

    def query(self,query) -> list[tuple]:
        """Return result of a SQL query.
        """

        try:
            res = self.cur.execute(query)
            is_successful = True
            return is_successful,res.fetchall()
        except:
            self.logger.debug(f"Query failed. {query=}")
            is_successful = False
            return is_successful, None


        


        

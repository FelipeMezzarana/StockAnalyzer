import sqlite3
from ..settings import Settings 
import os
from ..util.get_logger import get_logger
import shutil

class SQLiteHandler:
    """SQLLite db operations handler.
    """
    def __init__(self, settings: Settings):

        self.logger = get_logger(__name__, settings)
        self.settings = settings
        # Create db file if not exist
        self.logger.debug(f"{settings.DB_PATH=}")
        if not os.path.exists(settings.DB_PATH):
            self.logger.critical
            (f"Database file {settings.DB_PATH} not fount. A new file will be created"
             )
        
        self.conn = sqlite3.connect(settings.DB_PATH)
        # Save a recovery copy 
        shutil.copy(settings.DB_PATH, settings.DB_PATH.replace(".db",".recovery"))
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
        
    def create_table(self):
        """Create table based on pipeline settings"""

        # Build SQL statement
        table_name = self.settings.PIPELINE_TABLE["name"]
        table_fields = self.settings.PIPELINE_TABLE["fields_mapping"]
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} \n ("
        for v in table_fields.values():
            create_table_sql += f"{v[0]} {v[1]}\n, "

        create_table_sql = create_table_sql.strip(", ")
        create_table_sql += " )" 
        self.logger.debug(f"{create_table_sql=}")

        # Create Table
        self.cur.execute(create_table_sql)
        self.conn.commit()


        


        

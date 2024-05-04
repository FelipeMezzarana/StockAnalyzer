# Standard library
import os
import shutil
import sqlite3

# Local
from ..settings import Settings
from ..util.get_logger import get_logger


class SQLiteHandler:
    """SQLLite db operations handler."""

    def __init__(self, settings: Settings, recovery_copy: bool = False):
        self.logger = get_logger(__name__, settings)
        self.settings = settings
        # Create db file if not exist
        self.logger.debug(f"{settings.DB_PATH=}")
        if not os.path.exists(settings.DB_PATH):
            self.logger.critical
            (f"Database file {settings.DB_PATH} not fount. A new file will be created")
        self.conn = sqlite3.connect(settings.DB_PATH)
        # Save a recovery copy
        if recovery_copy:  # pragma: no cover
            shutil.copy(settings.DB_PATH, settings.DB_PATH.replace(".db", ".recovery"))
        self.cur = self.conn.cursor()
        self.logger.info("conected")
        # Create table if not exist
        self.create_table()

    def insert_into(self, raw_values: list[tuple], header: tuple):
        """Insert data into target table.
        Map field to expected position, see more in database_config.json
        """

        pipeline_table = self.settings.TABLES[self.settings.pipeline]
        table_name = pipeline_table["name"]
        fields_mapping = pipeline_table["fields_mapping"]
        fields = tuple(fields_mapping.keys())

        # Map fields to correct position.
        mapped_values_list = []
        for line in raw_values:
            raw_mapped_values = {h: v for h, v in zip(header, line)}
            mapped_values = tuple(raw_mapped_values.get(key) for key in fields)
            mapped_values_list.append(mapped_values)

        parameterized_fields = ("?, " * len(mapped_values_list[0])).strip(", ")
        query = f"INSERT INTO {table_name} VALUES({parameterized_fields})"
        self.cur.executemany(query, mapped_values_list)
        self.conn.commit()

    def query(self, query: str) -> tuple[bool, list]:
        """Return if query is successful and result of a SQL query."""

        try:
            res = self.cur.execute(query)
            return True, res.fetchall()
        except sqlite3.OperationalError as e:  # pragma: no cover
            self.logger.debug(f"Query failed. {e}. {query=}")
            return False, []

    def create_table(self):
        """Create table based on pipeline settings."""

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

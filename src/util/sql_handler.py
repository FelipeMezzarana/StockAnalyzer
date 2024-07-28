# Standard library
import sqlite3

# Local
from .get_logger import get_logger
from ..abstract.client import Client
from ..settings import Settings


class SQLHandler:
    """SQLLite db operations handler."""

    def __init__(self, settings: Settings, client: Client):
        self.logger = get_logger(__name__, settings)
        self.settings = settings
        self.client = client
        self.client.connect()

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
        self.client.executemany(query, mapped_values_list)

    def query(self, query: str) -> tuple[bool, list]:
        """Return if query is successful and result of a SQL query."""

        try:
            res = self.client.execute(query)
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
        self.client.execute(create_table_sql)

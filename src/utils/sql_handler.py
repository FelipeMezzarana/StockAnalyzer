# Standard library
import sqlite3
from typing import Optional

# Local
from .get_logger import get_logger
from ..abstract.client import Client
from ..settings import Settings


class SQLHandler:
    """Handle SQL operations for Postgres ans SQLite clients."""

    def __init__(self, settings: Settings, client: Client, table_config: Optional[dict] = None):
        """Settings setup.
        client_config -- Using this replaces the default settings config.
        """
        self.logger = get_logger(__name__, settings)
        self.settings = settings
        if table_config:
            self.pipeline_table = table_config
        else:
            self.pipeline_table = self.settings.PIPELINE_TABLE

        self.client_config = self.settings.CLIENT_CONFIG
        self.client = client
        self.create_table()

    def insert_into(self, raw_values: list[tuple], header: tuple):
        """Insert data into target table.
        Map field to expected position, see more in database_config.json
        """

        table_name = self.pipeline_table["schema"] + "." + self.pipeline_table["name"]
        fields_mapping = self.pipeline_table["fields_mapping"]
        fields = tuple(fields_mapping.keys())

        # Map fields to correct position.
        mapped_values_list = []
        for line in raw_values:
            raw_mapped_values = {h: v for h, v in zip(header, line)}
            mapped_values = tuple(raw_mapped_values.get(key) for key in fields)
            mapped_values_list.append(mapped_values)

        parameter_placeholder = self.client_config["PARAMETER_PLACEHOLDER"]
        parameterized_fields = (parameter_placeholder * len(mapped_values_list[0])).strip(", ")
        query = f"INSERT INTO {table_name} VALUES({parameterized_fields})"
        self.client.executemany(query, mapped_values_list)

    def query(self, query: str) -> tuple[bool, list]:
        """Return if query is successful and result of a SQL query."""

        try:
            res = self.client.execute(query)
            if res:
                return True, res
            else:
                self.logger.debug(f"None results for query: {query}.")
                return True, []
        except sqlite3.OperationalError as e:  # pragma: no cover
            self.logger.debug(f"Query failed. {e}. {query=}")
            return False, []

    def create_table(self):
        """Create table based on pipeline settings."""

        # Build SQL statement
        table_name = self.pipeline_table["schema"] + "." + self.pipeline_table["name"]
        table_fields = self.pipeline_table["fields_mapping"]
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} \n ("
        for v in table_fields.values():
            field_name = v[0]
            if self.client_config.get("TYPE_MAPPING"):  # pragma: no cover
                data_type = self.client_config["TYPE_MAPPING"][v[1]]
            else:
                data_type = v[1]
            create_table_sql += f"{field_name} {data_type}\n, "

        create_table_sql = create_table_sql.strip(", ")
        create_table_sql += " )"
        self.logger.debug(f"Creating table if not exist: {table_name}")

        # Create Table
        self.client.execute(create_table_sql)

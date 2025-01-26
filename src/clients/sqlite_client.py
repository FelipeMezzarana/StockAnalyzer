# Standard library
import os
import re
import sqlite3

# Local
from ..abstract.client import Client
from ..exceptions import DirectoryCreationError
from ..settings import Settings
from ..utils.constants import PIPELINES
from ..utils.get_logger import get_logger


class SQLiteClient(Client):
    """SQLite Client."""

    def __init__(self, settings: Settings):
        """Setup settings."""

        self.logger = get_logger(__name__, settings)
        self.logger.debug(f"Initializing SQLiteClient for pipeline {settings.pipeline}")
        self.settings = settings
        self.DB_PATH = settings.CLIENT_CONFIG["DB_PATH"]
        # Create db file if not exist
        self._check_db(self.DB_PATH)
        self.logger.debug(f"{self.DB_PATH=}")
        self.connect()
        for schema in PIPELINES.schemas:
            self._create_schema(schema)

    def connect(self):
        """Connect to db."""
        self.conn = sqlite3.connect(self.DB_PATH)
        self.cur = self.conn.cursor()
        self.logger.info("conected")

    def execute(self, query: str) -> list[tuple]:
        """Execute query."""

        resp = self.cur.execute(query)
        self.conn.commit()

        return resp.fetchall()

    def executemany(self, query: str, mapped_values: list[tuple]):
        """Execute parameterized query."""
        self.cur.executemany(query, mapped_values)
        self.conn.commit()

    def _check_db(self, db_path):
        """Check if SQLite file exist, creates a new one if not."""

        if not os.path.exists(db_path):
            self.logger.info
            (f"Database file {db_path} not fount. A new file will be created")
            # Check directory
            if re.search("/", db_path):
                try:
                    directory = re.findall(r"([^\/]*)/[^\/]*.db", db_path)[0]
                    if not os.path.exists(directory):
                        os.mkdir(directory)
                        self.logger.info(f"DB directory not found. Created {directory}")
                    else:
                        self.logger.info(f"DB directory found. {directory=}")
                except Exception as err:  # pragma: no cover
                    raise DirectoryCreationError(directory, err)

    def _schema_exist(self, schema_name: str) -> bool:
        """Check if schema exists."""
        query = f"""
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name='{schema_name}';
        """
        result = self.execute(query)
        return len(result) > 0

    def _create_schema(self, schema_name: str):
        """Create schema."""

        if not self._schema_exist(schema_name):
            query = f"""
            ATTACH DATABASE '{schema_name}' AS '{schema_name}';
            """
            self.execute(query)
            self.logger.info(f"Created schema {schema_name}")
        else:  # pragma: no cover
            self.logger.info(f"Schema {schema_name} already exists")

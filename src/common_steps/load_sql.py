# Standard library
import os
from typing import Dict

# Third party
import duckdb

# Local
from ..abstract.client import Client
from ..abstract.step import Step
from ..settings import Settings
from ..util.csv_handler import clean_temp_file
from ..util.sql_handler import SQLHandler


class SQLLoader(Step):
    """Load data into SQLite DB."""

    def __init__(self, previous_output: dict, settings: Settings, client: Client) -> None:
        """Init loader."""
        super(SQLLoader, self).__init__(__name__, previous_output, settings)

        self.sqlite_client = SQLHandler(settings, client)
        self.valid_file_path = self.previous_output["valid_file_path"]
        self.invalid_file_path = self.previous_output["invalid_file_path"]
        self.chunk_size = settings.CLIENT_CONFIG["CHUNK_SIZE"]

    def run(self, clean_file: bool = True) -> tuple[bool, Dict]:
        """run step."""

        if not os.path.isfile(self.valid_file_path):  # pragma: no cover
            self.logger.info("No file to load.")
            return True, self.output

        raw_file = duckdb.read_csv(self.valid_file_path, header=True)
        header = duckdb.read_csv(self.valid_file_path, header=False).fetchone()
        if not header:
            raise ValueError(f"Error reading {self.valid_file_path}")

        next_chunk = True
        chunk, rows = 1, 0
        while next_chunk:
            raw_values = []
            for _ in range(self.chunk_size):
                line = raw_file.fetchone()
                if line:
                    raw_values.append(line)
                    rows += 1
                else:
                    next_chunk = False
                    break
            self.sqlite_client.insert_into(raw_values, header)
            self.logger.info(f"{rows:,} Values inserted. {chunk=}")
            chunk += 1

        if clean_file:  # pragma: no cover
            clean_temp_file(self.valid_file_path)

        return True, self.output

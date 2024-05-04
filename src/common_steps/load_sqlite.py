# Standard library
import os

# Third party
import duckdb

# Local
from ..abstract.step import Step
from ..clients.sqlite_handler import SQLiteHandler
from ..settings import Settings
from ..util.csv_handler import clean_temp_file


class SQLiteLoader(Step):
    """Load data into SQLite DB."""

    def __init__(self, previous_output: dict, settings: Settings) -> None:
        """Init loader."""
        super(SQLiteLoader, self).__init__(__name__, previous_output, settings)

        self.sqlite_client = SQLiteHandler(settings)
        self.valid_file_path = self.previous_output["valid_file_path"]
        self.invalid_file_path = self.previous_output["invalid_file_path"]
        self.chunk_size = settings.CHUNK_SIZE

    def run(self, clean_file: bool = True):
        """run step."""

        if not os.path.isfile(self.valid_file_path):  # pragma: no cover
            self.logger.info("No file to load.")
            return True, self.output

        raw_file = duckdb.read_csv(self.valid_file_path, header=True)
        header = duckdb.read_csv(self.valid_file_path, header=False).fetchone()

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

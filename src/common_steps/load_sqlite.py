from ..settings import Settings
from ..util.get_logger import get_logger
from ..abstract.step import Step
import duckdb
from ..clients.sqlite_handler import SQLiteHandler

class SQLiteLoader(Step):

    def __init__(self,previous_output: dict, settings: Settings) -> None:
        """Init loader."""
        super(SQLiteLoader, self).__init__(__name__, previous_output, settings)
        
        self.sqlite_client = SQLiteHandler(settings)
        self.valid_file_path = self.previous_output["valid_file_path"] 
        self.invalid_file_path = self.previous_output["invalid_file_path"]
        self.chunk_size = settings.CHUNK_SIZE

    def run(self):
        """run step."""

        raw_file = duckdb.read_csv(self.valid_file_path, header = True)
        header = duckdb.read_csv(self.valid_file_path, header = False).fetchone()

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

        return True, self.output
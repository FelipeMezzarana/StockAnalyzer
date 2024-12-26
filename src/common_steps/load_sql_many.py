# Local
from .load_sql import SQLLoader
from ..abstract.client import Client
from ..abstract.step import Step
from ..settings import Settings


class SQLManyLoader(Step):
    """Load data into multiple client db tables."""

    def __init__(self, previous_output: dict, settings: Settings, client: Client):
        """Init validator."""
        super(SQLManyLoader, self).__init__(__name__, previous_output, settings)

        self.pipeline_tables = self.settings.PIPELINE_TABLE
        self.client = client

    def run(self):
        """Run validation step for many tables."""
        for table in self.pipeline_tables:
            table_files = self.previous_output[table["name"]]
            context = {
                "valid_file_path": table_files["valid_file_path"],
                "invalid_file_path": table_files["invalid_file_path"],
            }
            sql_loader = SQLLoader(context, self.settings, self.client, table)
            _, output = sql_loader.run()
            self.output.update({table["name"]: output})

        return True, self.output

# Standard library
import csv
import os
from datetime import date, datetime

# Third party
import duckdb

# Local
from ..abstract.step import Step
from ..settings import Settings
from ..utils.csv_handler import clean_temp_file


class Validator(Step):
    """Validate file.
    Validation based in:
       * Current pipeline settings
       * previous_output["file_path"]
    """

    def __init__(self, previous_output: dict, settings: Settings, table_config: dict = {}):
        """Init validator.
        table_config -- Optional. If not provided, will use settings.PIPELINE_TABLE.
        Must have fields_mapping and required_fields as keys.
        """
        super(Validator, self).__init__(__name__, previous_output, settings)

        self.file_path: str = self.previous_output["file_path"]
        self.pipeline: str = self.settings.pipeline
        if not table_config:
            table_config = self.settings.PIPELINE_TABLE

        self.fields_mapping: dict = table_config["fields_mapping"]
        self.required_fields: list = table_config["required_fields"]

    def _get_python_type(self, sqlite_dtype) -> type:
        """Return python data type."""

        dtype_mapping = {
            "VARCHAR(255)": str,
            "FLOAT": float,
            "INTEGER": int,
            "DATETIME": datetime,
            "DATE": date,
        }

        return dtype_mapping[sqlite_dtype]

    def _is_valid(self, fields: list, line: list):
        """Checks is required field exists."""

        for field, value in zip(fields, line):
            if field in self.required_fields and not self._is_dtype_valid(field, value):
                return False
        return True

    def _is_dtype_valid(self, field: str, value):
        """Validate line based on src/database_config.json fields mappings."""

        expected_dtype = self._get_python_type(self.fields_mapping[field][1])
        if not isinstance(value, expected_dtype):
            self.logger.info(f"Invalid data found. {field=} | {value=}")
            return False
        else:
            return True

    def run(self):
        """Run validation step."""

        self.output["valid_file_path"] = self.file_path.replace(".csv", "_valid.csv")
        self.output["invalid_file_path"] = self.file_path.replace(".csv", "_invalid.csv")

        if not os.path.isfile(self.file_path):  # pragma: no cover
            self.logger.info("No file to validade.")
            return True, self.output

        raw_file = duckdb.read_csv(self.file_path, header=True)
        header = duckdb.read_csv(self.file_path, header=False).fetchone()
        invalid_file_exist = os.path.isfile(self.output.get("invalid_file_path"))

        with (
            open(self.output["valid_file_path"], "a", newline="") as valid_file,
            open(self.output["invalid_file_path"], "a", newline="") as invalid_file,
        ):
            valid_csv_writer = csv.writer(valid_file)
            invalid_csv_writer = csv.writer(invalid_file)
            valid_csv_writer.writerow(header)
            if not invalid_file_exist:
                invalid_csv_writer.writerow(header)
            # Iterete results without loading in memory
            n_lines, err_count = 0, 0
            while True:
                line = raw_file.fetchone()
                if not line:
                    break
                if self._is_valid(header, line):
                    valid_csv_writer.writerow(line)
                else:
                    invalid_csv_writer.writerow(line)
                    err_count += 1
                n_lines += 1

        clean_temp_file(self.file_path)

        self.logger.info(f"Validation complete. {err_count} Invalid lines found.")
        return True, self.output

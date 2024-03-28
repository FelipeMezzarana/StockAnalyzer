from ..settings import Settings
from ..util.get_logger import get_logger
from ..util.csv_handler import clean_temp_file
import csv
import duckdb
from datetime import datetime, date
import os
from ..abstract.step import Step


class Validator(Step):
    """Validade file.
     Validation based in: 
        * Current pipeline settings
        * previous_output["file_path"]
    """

    def __init__(self,previous_output: dict, settings: Settings):
        """Init validator."""
        super(Validator, self).__init__(__name__, previous_output, settings)

        self.file_path = self.previous_output.get("file_path")
        self.pipeline = self.settings.pipeline
        self.pipeline_table = self.settings.TABLES.get(self.pipeline)
        self.fields_mapping = self.pipeline_table.get("fields_mapping")
        self.required_fields = self.pipeline_table.get("required_fields")


    def _get_python_type(self, sqlite_dtype) -> str:
        """Return python data type.
        """

        dtype_mapping = {
              "VARCHAR(255)":str,
              "FLOAT": float,
              "INTEGER": int,
              "DATETIME": datetime,
              "DATE": date
        }

        return dtype_mapping.get(sqlite_dtype)

    def _is_valid(self, fields: list, line: list):
        """Checks is required fild exists.
        """

        for field, value in zip(fields, line):
            if field in self.required_fields and not self._is_dtype_valid(field, value):
                return False
        return True


    def _is_dtype_valid(self, field: str, value):
        """Validate line based on settings.py fields mappings.
        """
        
        expected_dtype =  self._get_python_type(self.fields_mapping.get(field)[1])
        if not isinstance(value, expected_dtype):
            self.logger.info(f"Invalid data found. {field=} | {value=}")
            return False
        else:
            return True
    
            
    def run(self):
        """Run validation step."""

        self.output["valid_file_path"] = self.file_path.replace(".csv","_valid.csv")
        self.output["invalid_file_path"] = self.file_path.replace(".csv","_invalid.csv")

        if not os.path.isfile(self.file_path):
            self.logger.info("No file to validade.")
            return True, self.output

        raw_file = duckdb.read_csv(self.file_path, header = True)
        header = duckdb.read_csv(self.file_path, header = False).fetchone()
        invalid_file_exist = os.path.isfile(self.output["invalid_file_path"])
        
        with (open(self.output["valid_file_path"], 'a', newline='') as valid_file, 
              open(self.output["invalid_file_path"], 'a', newline='') as invalid_file):
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


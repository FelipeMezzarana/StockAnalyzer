from ..settings import Settings
from ..util.get_logger import get_logger
from ..util.csv_handler import append_line
import csv


class Validator:
    """Validade file.
     Validation based in: 
        * Current pipeline settings
        * previous_output["file_path"]
    """

    def __init__(self,previous_output: dict, settings: Settings):

        self.logger = get_logger(__name__, settings)
        self.output = {}
        self.settings = settings
        self.previous_output = previous_output
        self.file_path = self.previous_output["file_path"]
        self.pipeline = self.settings.pipeline
        self.pipeline_table = self.settings.TABLES.get(self.pipeline)
        self.fields_mapping = self.pipeline_table.get("fields_mapping")


    def _get_python_type(self, sqlite_dtype) -> str:
        """Return python data type.
        """

        dtype_mapping = {
              "VARCHAR(255)":str,
              "FLOAT": float,
              "INTEGER": int,
              "DATETIME": (str, int) # Datetime from csv
        }

        return dtype_mapping.get(sqlite_dtype)


    def _is_valid(self, fields: list, line: list):
        """Validate line based on settings.py fields mappings.
        """
        
        is_valid = True
        for field, value in zip(fields, line):
            # Check settings.py for better understanding.
            
            expected_dtype =  self._get_python_type(self.fields_mapping.get(field)[1])
            # Try to convert str to dtype
            if expected_dtype in (float, int):
                str_digt = value.replace(".","")
                is_valid = str_digt.isdigit()
            elif not isinstance(value, expected_dtype):
                is_valid = False
                self.logger.info(f"Invalid data found. {field=} | {value=}")
                break 
  
        return is_valid
            
    def run(self):
        """Run validation step."""

        self.output["valid_file_path"] = self.file_path.replace(".csv","_valid.csv")
        self.output["invalid_file_path"] = self.file_path.replace(".csv","_invalid.csv")

        with open(self.file_path, newline='') as raw_file:
            csv_reader = csv.reader(raw_file)
            n_lines = 0
            for ln in csv_reader:
                if n_lines == 0:
                    fields = ln
                    n_lines += 1
                else:
                    if self._is_valid(fields, ln):
                        append_line(self.output["valid_file_path"], ln, fields)
                    else:
                        append_line(self.output["invalid_file_path"], ln, fields)
                    n_lines += 1

        return True, self.output


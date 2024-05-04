# Standard library
import csv
import os
from typing import Optional, List, Dict

def append_to_file(file_path: str, data: List[Dict], header: Optional[List[str]] = None):
    """Append data to csv file.
    Creates file if not exist.

    If header is not specified, use dict keys.
    """

    file_exists = os.path.isfile(file_path)
    with open(file_path, "a", newline="") as f:
        if not header:
            header = list(data[0].keys())

        csv_writer = csv.DictWriter(f, delimiter=",", lineterminator="\n", fieldnames=header)
        # Check File
        if not file_exists:
            csv_writer.writeheader()
        # Append data
        for dictionary in data:
            row =  {h:dictionary.get(h) for h in header} 
            csv_writer.writerow(row)

def clean_temp_file(file_path: str):
    """Delete temp file."""

    if os.path.isfile(file_path):
        os.remove(file_path)

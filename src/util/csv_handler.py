
import csv
import os
from ..util.get_logger import get_logger


def append_to_file(file_path: str, data:list[dict]):
    """Append data to csv file.
    Creates file if not exist.
    """

    file_exists = os.path.isfile(file_path)
    with open(file_path, 'a', newline='') as f:
        # Get header
        fields = list(data[0].keys())
        csv_writer = csv.DictWriter(f, delimiter=',', lineterminator='\n', fieldnames=fields)
        # Check File
        if not file_exists:
            csv_writer.writeheader()
        # Append data
        for dictionary in data:
            csv_writer.writerow(dictionary)


def clean_temp_file(file_path: str):
    """Delete temp file."""
     
    if os.path.isfile(file_path):
        os.remove(file_path)


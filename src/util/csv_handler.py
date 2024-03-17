
import csv
import os

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

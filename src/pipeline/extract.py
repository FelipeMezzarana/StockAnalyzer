
from ..clients.polygon import Polygon
from ..clients.sqlite_handler import SQLiteHandler
from validate import Validator

class Extractor:
    """Extract data from polygon API
    """

    def __init__(self, max_days_hist: int = 735):
        """Initiate clients and settings.
        
        max_days_hist -- max historical data covered by API plan, in days.
        """

        self.polygon_client = Polygon()
        self.sqlite_client = SQLiteHandler()
        self.validator = Validator()

        self.max_days_hist = max_days_hist
        
    def get_last_date(table_name: str, date_col: str):
        """Return max date for any given table.
        Query SQLite DB defined in src.settings.
        """


    def update_grouped_daily(self,start_date: str,end_date: str):
        """Return 

        start_date -- start of period (format yyyy-mm-dd)
        end_date -- end of period (format yyyy-mm-dd)
        """


    

from ....clients.polygon import Polygon
from ....clients.sqlite_handler import SQLiteHandler
from .... import settings
from datetime import datetime, timedelta
from ....util.get_logger import get_logger


class GroupedDailyExtractor:
    """Extract daily data from polygon API
    """

    def __init__(self, previous_output: dict, max_days_hist: int = 735):
        """Initiate clients and settings.
        
        max_days_hist -- max historical data covered by API plan, in days.
        """
        self.logger = get_logger(__name__)
        self.previous_output = previous_output
        
        self.polygon_client = Polygon()
        self.sqlite_client = SQLiteHandler()
        self.max_days_hist = max_days_hist
        
    def get_last_date(self):
        """Return max date for grouped_daily table.
        Query SQLite DB defined in src.settings.
        """

        table_name = settings.TABLE_NAMES['grouped-daily-pipeline']["name"]
        is_successful, max_date = self.sqlite_client.query(
            f"SELECT MAX(DATE) FROM {table_name}"
            )
        
        if is_successful:
            return max_date
        else:
            max_hist_avaiable = (
                datetime.today() - 
                timedelta(days=self.max_days_hist)
                ).strftime("%Y-%m-%d")
            return max_hist_avaiable

    def update_grouped_daily(self,start_date: str,end_date: str):
        """Return 

        start_date -- start of period (format yyyy-mm-dd)
        end_date -- end of period (format yyyy-mm-dd)
        """

        pass
    
    def run(self):
        """Run step."""

        max_date = self.get_last_date()
        self.logger.info(f"{max_date=}")

        output = {"test": "ok"}
        return True, output
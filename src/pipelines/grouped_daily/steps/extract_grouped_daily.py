# Standard library
from datetime import datetime, timedelta

# Local
from ....abstract.step import Step
from ....clients.polygon import Polygon
from ....clients.sqlite_handler import SQLiteHandler
from ....settings import Settings
from ....util.csv_handler import append_to_file

class GroupedDailyExtractor(Step):
    """Extract daily data from polygon API."""

    def __init__(self, previous_output: dict, settings: Settings):
        """Initiate clients and settings.

        max_days_hist -- max historical data covered by API plan, in days.
        """
        super(GroupedDailyExtractor, self).__init__(__name__, previous_output, settings)

        self.polygon_client = Polygon(settings)
        self.sqlite_client = SQLiteHandler(settings)
        self.max_days_hist = self.settings.POLYGON_MAX_DAYS_HIST

    def get_last_date(self):
        """Return max date for grouped_daily table.
        Query SQLite DB defined in src.settings.
        """

        table_name = self.settings.PIPELINE_TABLE["name"]
        is_successful, last_date = self.sqlite_client.query(f"SELECT MAX(DATE) FROM {table_name}")

        last_update_date = last_date[0][0] if last_date else None
        if is_successful and last_update_date:
            return last_update_date
        else:
            max_hist_avaiable = (datetime.today() - timedelta(days=self.max_days_hist)).strftime(
                "%Y-%m-%d"
            )
            if last_date:
                return max_hist_avaiable
            else:
                self.logger.info(f"Table {table_name} not found. Creating from scratch")
                self.sqlite_client.create_table()
                return max_hist_avaiable

    def update_grouped_daily(self, start_date: str, avoid_weeknds: bool = True):
        """Update multiple dates in GROUPED_DAILY table.

        start_date -- start of period (format yyyy-mm-dd)
        end_date -- end of period (format yyyy-mm-dd)
        """
        
        request_date = start_date
        file_path = "temp/grouped_daily_temp.csv"
        self.output["file_path"] = file_path
        api_call_count, row_count = 0, 0
        while request_date != self.settings.POLYGON_UPDATE_UNTIL:
            if not avoid_weeknds or not self._is_weekend(request_date):
                result = self.polygon_client.get_grouped_daily(request_date)
                data = result.get("results")
                if data:
                    for stock_dict in data:
                        # Add extra filds
                        stock_dict["date"] = request_date
                        stock_dict["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    append_to_file(file_path, data)
            data_obj = datetime.strptime(request_date, '%Y-%m-%d')
            request_date = (data_obj + timedelta(days=1)).strftime('%Y-%m-%d')
            api_call_count += 1
            row_count += result.get("resultsCount", 0)

    def _is_weekend(self, date: str) -> bool:
        """Check if string date is weekend.
        date -- format yyyy-mm-dd
        """

        data_obj = datetime.strptime(date, '%Y-%m-%d')
        if data_obj.weekday() in [5,6]:
            return True
        else:
            return False

    def run(self):
        """Run step."""

        last_update_date = self.get_last_date()
        self.logger.info(f"{last_update_date=}")
        self.update_grouped_daily(last_update_date)


        self.output["test"] = "ok"
        return True, self.output

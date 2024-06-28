# Standard library
from datetime import datetime

# Local
from ....abstract.step import Step
from ....clients.sqlite_handler import SQLiteHandler
from ....settings import Settings


class IndexDailyCloseChecker(Step):
    """Check status of INDEXES_DAILY_CLOSE."""

    def __init__(self, previous_output: dict, settings: Settings):
        """Init class."""
        super(IndexDailyCloseChecker, self).__init__(__name__, previous_output, settings)
        self.sqlite_client = SQLiteHandler(settings)
        self.indexes = settings.FRED["INDEXES"]

    def get_last_update(self) -> dict:
        """Return last update date each index in table INDEXES_DAILY_CLOSE."""

        is_successful, result = self.sqlite_client.query(
            "SELECT index_code, max(date) as last_DATE FROM INDEXES_DAILY_CLOSE GROUP BY index_code"
        )
        if is_successful:
            last_update = dict(result)
            indexes_last_update = {k: last_update.get(k, "1600-01-01") for k in self.indexes}
            self.logger.debug(f"Indexes last updated: {indexes_last_update}")
            return indexes_last_update
        else:  # pragma: no cover
            raise KeyError("INDEXES_DAILY_CLOSE Table no found.")

    def run(self):
        """Run step."""

        indexes_last_update = self.get_last_update()
        last_update_str = min(indexes_last_update.values())
        last_update = datetime.strptime(last_update_str, "%Y-%m-%d")
        today = datetime.today()
        days_past = (today - last_update).days
        if days_past > 0:
            self.output["skip_pipeline"] = False
            self.output["indexes_last_update"] = indexes_last_update
            self.logger.info(
                f"Table not updated since {last_update_str}, {days_past} ago. Running pipeline."
            )
        else:
            self.output["skip_pipeline"] = True
            self.logger.info(f"Table up to date. Updated {days_past} days ago. Skipping pipeline")

        return True, self.output

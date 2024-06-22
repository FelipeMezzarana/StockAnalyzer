# Standard library
from datetime import datetime

# Local
from ....abstract.step import Step
from ....clients.sqlite_handler import SQLiteHandler
from ....settings import Settings


class SP500Checker(Step):
    """Check status of SP500_BASIC_DETAILS."""

    def __init__(self, previous_output: dict, settings: Settings):
        """Init class."""
        super(SP500Checker, self).__init__(__name__, previous_output, settings)
        self.sqlite_client = SQLiteHandler(settings)

    def get_last_update(self) -> datetime:
        """Return last update date for table SP500_BASIC_DETAILS."""

        is_successful, last_update_result = self.sqlite_client.query(
            "SELECT max(updated_at) as last_update FROM SP500_BASIC_DETAILS"
        )
        if is_successful:
            last_update = last_update_result[0][0]
            if last_update:
                self.logger.info(f"SP500_BASIC_DETAILS {last_update=}")
                return datetime.strptime(last_update, "%Y-%m-%d %H:%M:%S")
            else:  # pragma: no cover
                self.logger.info("SP500_BASIC_DETAILS Table empty.")
                return datetime(1990, 1, 1)
        else:  # pragma: no cover
            raise KeyError("SP500_BASIC_DETAILS Table no found.")

    def run(self):
        """Run step."""

        last_update = self.get_last_update()
        today = datetime.today()
        if (today - last_update).days > 30:
            self.output["skip_pipeline"] = False
            self.logger.info("Table not updated in past 30 days. Running pipeline.")
        else:
            self.output["skip_pipeline"] = True
            self.logger.info(
                f"Table up to date. Updated {(today - last_update).days} days ago. Skipping"
                " pipeline"
            )

        return True, self.output

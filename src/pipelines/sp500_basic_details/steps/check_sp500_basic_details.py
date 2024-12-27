# Standard library
from datetime import datetime

# Local
from ....abstract.client import Client
from ....abstract.step import Step
from ....settings import Settings
from ....utils.sql_handler import SQLHandler


class SP500Checker(Step):
    """Check status of SP500_BASIC_DETAILS."""

    def __init__(self, previous_output: dict, settings: Settings, client: Client):
        """Init class."""
        super(SP500Checker, self).__init__(__name__, previous_output, settings)
        self.sqlite_client = SQLHandler(settings, client)

    def get_last_update(self) -> datetime:
        """Return last update date for table SP500_BASIC_DETAILS."""

        is_successful, last_update_result = self.sqlite_client.query(
            "SELECT max(updated_at) as last_update FROM BRONZE_LAYER.SP500_BASIC_DETAILS"
        )
        if is_successful:
            if last_update_result and last_update_result[0][0]:
                last_update = last_update_result[0][0]
                self.logger.info(f"SP500_BASIC_DETAILS {last_update=}")
                if isinstance(last_update, str):
                    return datetime.strptime(last_update, "%Y-%m-%d %H:%M:%S")
                elif isinstance(last_update, datetime):  # pragma: no cover
                    return last_update
                else:  # pragma: no cover
                    raise ValueError(
                        f"Unexpected date type for: {last_update} type: {type(last_update)}"
                    )
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

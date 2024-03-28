# Standard library
import logging
from datetime import datetime

# Pipelines to run, in order.
PIPELINES = [
    "grouped-daily-pipeline"
    ]

class Settings:
    """Handles settings through the application."""

    def __init__(self, pipeline: str) -> None:
        """Defines general app settings."""

        self.LOGGING_LEVEL = logging.DEBUG

        # Pipeline and step settings
        if pipeline not in PIPELINES: # pragma: no cover
            raise ValueError(
                f"pipiline must be one of {PIPELINES}. {pipeline} is not valid."
                )
        self.pipeline = pipeline
        self.step_name = None  # Placeholder
        # Each pipeline generally update one table
        self.TABLES = {
            "grouped-daily-pipeline": {
                "name": "GROUPED_DAILY",
                "fields_mapping": {
                    "date": ("date", "DATE"),
                    "T": ("exchange_symbol", "VARCHAR(255)"),
                    "v": ("trading_volume", "FLOAT"),
                    "vw": ("volume_weighted_avg", "FLOAT"),
                    "o": ("open_price", "FLOAT"),
                    "c": ("close_price", "FLOAT"),
                    "h": ("highest_price", "FLOAT"),
                    "l": ("lowest_price", "FLOAT"),
                    "t": ("end_window", "DATETIME"),
                    "n": ("n_transaction", "INTEGER"),
                    "updated_at": ("updated_at", "DATETIME"),
                },
                "required_fields": [
                    "date",
                    "T",
                    "o",
                    "c",
                    "h",
                    "l",
                    "t",
                    "updated_at"
                ]
            }
        }
        self.PIPELINE_TABLE = self.TABLES.get(pipeline)

        # Database settings
        self.DB_PATH = "src/database/stock_database.db"
        self.BASE_URL = "https://api.polygon.io/"
        self.ENDPOINTS = {"grouped_daily_endpoint": "v2/aggs/grouped/locale/us/market/stocks/"}
        self.CHUNK_SIZE = 50000

        # Polygon API settings
        self.POLYGON_MAX_DAYS_HIST = 730
        self.POLYGON_CALLS_PER_MIN = 5
        # Free API allows calls only until the end of the previous day
        self.POLYGON_UPDATE_UNTIL = datetime.today().strftime('%Y-%m-%d')

        


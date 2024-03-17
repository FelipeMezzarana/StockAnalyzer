# Standard library
import logging
from datetime import datetime, timedelta

class Settings:
    """Handles settings through the application."""

    def __init__(self, pipeline: str) -> None:
        """Defines general app settings."""

        self.LOGGING_LEVEL = logging.DEBUG

        # Pipeline and step settings
        self.pipeline = pipeline
        self.step_name = None  # Placeholder
        # Each pipeline generally update one table
        self.TABLES = {
            "grouped-daily-pipeline": {
                "name": "GROUPED_DAILY",
                "fields_mapping": {
                    "non_api1": ("date", "DATETIME"),
                    "T": ("exchange_symbol", "VARCHAR(255)"),
                    "v": ("trading_volume", "FLOAT"),
                    "vw": ("volume_weighted_avg", "FLOAT"),
                    "o": ("open_price", "FLOAT"),
                    "c": ("close_price", "FLOAT"),
                    "h": ("highest_price", "FLOAT"),
                    "l": ("lowest_price", "FLOAT"),
                    "t": ("end_window", "DATETIME"),
                    "n": ("n_transaction", "INTEGER"),
                    "non_api2": ("updated_at", "DATETIME"),
                },
            }
        }
        self.PIPELINE_TABLE = self.TABLES.get(pipeline)

        # Database settings
        self.DB_PATH = "src/database/stock_database.db"
        self.BASE_URL = "https://api.polygon.io/"
        self.ENDPOINTS = {"grouped_daily_endpoint": "v2/aggs/grouped/locale/us/market/stocks/"}

        # Polygon API settings
        self.POLYGON_MAX_DAYS_HIST = 4
        self.POLYGON_CALLS_PER_MIN = 5
        # Free API allows calls only until the end of the previous day
        self.POLYGON_UPDATE_UNTIL = datetime.today().strftime('%Y-%m-%d')

        


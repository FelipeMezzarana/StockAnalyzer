# Standard library
import json
import logging
from datetime import datetime
from typing import Any, Dict

# Pipelines to run, in order.
PIPELINES = ["grouped-daily-pipeline", "ticker-basic-details-pipeline"]


class Settings:
    """Handles settings through the application."""

    def __init__(self, pipeline: str) -> None:
        """Defines general app settings."""

        self.LOGGING_LEVEL = logging.DEBUG

        # Pipeline and step settings
        if pipeline not in PIPELINES:  # pragma: no cover
            raise ValueError(f"pipiline must be one of {PIPELINES}. {pipeline} is not valid.")
        self.pipeline = pipeline
        self.step_name = None  # Placeholder

        # Database settings
        with open("src/database_config.json", "r") as file:
            self.TABLES: Dict[str, Any] = json.load(file)
        self.PIPELINE_TABLE: Dict[str, Any] = self.TABLES[pipeline]
        self.DB_PATH = "src/database/stock_database.db"
        self.CHUNK_SIZE = 50000

        # Polygon API settings
        self.BASE_URL = "https://api.polygon.io/"
        self.ENDPOINTS = {
            "grouped_daily_endpoint": "v2/aggs/grouped/locale/us/market/stocks/",
            "ticker_basic_details_endpoint": "v3/reference/tickers?limit=1000",
        }
        self.POLYGON_MAX_DAYS_HIST = 730
        self.POLYGON_CALLS_PER_MIN = 5
        self.MAX_PAGINATION = 5
        # Free API allows calls only until the end of the previous day
        self.POLYGON_UPDATE_UNTIL = datetime.today().strftime("%Y-%m-%d")

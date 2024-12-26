# Standard library
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict

# Local
from .exceptions import InvalidClientError, InvalidPipelineError
from .utils.constants import AVAILABLE_CLIENTS, PIPELINES


class Settings:
    """Handles settings through the application."""

    def __init__(self, pipeline: str) -> None:
        """Defines general app settings."""

        self.LOGGING_LEVEL = logging.DEBUG
        self.is_integration_test = False
        # Pipeline and step settings
        if pipeline not in PIPELINES:  # pragma: no cover
            raise InvalidPipelineError(pipeline)
        self.pipeline = pipeline
        self.step_name = None  # Placeholder

        # Database settings
        with open("src/database_config.json", "r") as file:
            self.TABLES: Dict[str, Any] = json.load(file)
        self.PIPELINE_TABLE = self.TABLES[pipeline]

        # Client settings
        self.CLIENTS_CONFIG: dict = {
            "SQLITE": {
                "DB_PATH": os.getenv("DB_PATH", "") + "stock_database.db",
                "CHUNK_SIZE": 50000,
                "PARAMETER_PLACEHOLDER": "?, ",
            },
            "POSTGRES": {
                "POSTGRES_USER": os.getenv("POSTGRES_USER"),
                "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
                "POSTGRES_DB": os.getenv("POSTGRES_DB"),
                "POSTGRES_HOST": os.getenv("POSTGRES_HOST"),
                "CHUNK_SIZE": 50000,
                "PARAMETER_PLACEHOLDER": r"%s, ",
                "TYPE_MAPPING": {
                    "VARCHAR(255)": "text",
                    "FLOAT": "float",
                    "DATETIME": "timestamp",
                    "DATE": "timestamp",
                    "INTEGER": "int",
                    "BOOLEAN": "bool",
                },
            },
        }

        # Get current client config
        self.CLIENT = os.getenv("CLIENT", "SQLITE")
        if self.CLIENT not in AVAILABLE_CLIENTS:  # pragma: no cover
            raise InvalidClientError(self.CLIENT)
        self.CLIENT_CONFIG = self.CLIENTS_CONFIG[self.CLIENT]

        # Polygon API settings
        self.POLYGON: dict = {
            "BASE_URL": "https://api.polygon.io/",
            "ENDPOINTS": {
                "grouped_daily_endpoint": "v2/aggs/grouped/locale/us/market/stocks/",
                "ticker_basic_details_endpoint": "v3/reference/tickers?limit=1000",
                "financials_endpoint": "vX/reference/financials?",
            },
            "POLYGON_MAX_DAYS_HIST": 730,
            "POLYGON_CALLS_PER_MIN": 5,
            "MAX_PAGINATION": 5,
            # Free API allows calls only until the end of the previous day
            "POLYGON_UPDATE_UNTIL": datetime.today().strftime("%Y-%m-%d"),
        }

        # Fred API settings
        self.FRED: dict = {
            "BASE_URL": "https://api.stlouisfed.org/fred/",
            "INDEXES": ["SP500", "DJIA", "NASDAQ100", "NASDAQCOM", "DJTA", "DJCA", "DJUA"],
            "ENDPOINTS": {"index_daily_close": "series/observations?"},
        }

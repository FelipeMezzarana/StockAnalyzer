import logging


LOGGING_LEVEL = logging.DEBUG

# Pipelines to run, in order.
PIPELINES = [
    "grouped-daily-pipeline"
]

# Each pipeline generally update one table
TABLE_NAMES = {
    "grouped-daily-pipeline":{
        "name": "GROUPED_DAILY",
        "fields_mapping": {
            "T": "exchange_symbol",
            'v': "trading_volume",
            'vw': "volume_weighted_avg",
            'o': "open_price",
            'c': "close_price",
            'h': "highest_price",
            'l': "lowest_price",
            't': "end_window_timestamp",
            'n': "n_transaction"
            }
        }
    }

# General settings
DB_PATH = "src/database/stock_database.db"
BASE_URL = "https://api.polygon.io/"
ENDPOINTS = {
    "grouped_daily_endpoint": "v2/aggs/grouped/locale/us/market/stocks/"
    }


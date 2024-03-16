import logging



class Settings:

    def __init__(self, pipeline: str) -> None:

        self.LOGGING_LEVEL = logging.DEBUG

        self.pipeline = pipeline
        # Each pipeline generally update one table
        self.TABLES = {
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
        self.PIPELINE_TABLE = self.TABLES.get(pipeline)

        # General settings
        self.DB_PATH = "src/database/stock_database.db"
        self.BASE_URL = "https://api.polygon.io/"
        self.ENDPOINTS = {
            "grouped_daily_endpoint": "v2/aggs/grouped/locale/us/market/stocks/"
            }
        self.MAX_DAYS_HIST = 2

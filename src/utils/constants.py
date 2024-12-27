# Pipelines to run, in order.
PIPELINES = [
    "grouped-daily-pipeline",
    "ticker-basic-details-pipeline",
    "sp500-basic-details-pipeline",
    "indexes-daily-close-pipeline",
    "financials-pipeline",
]

AVAILABLE_CLIENTS = ["SQLITE", "POSTGRES"]

SCHEMAS = [
    "bronze_layer",
    "silver_layer",
    "gold_layer",
]

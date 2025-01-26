class Pipelines:
    """Pipelines class to store allowed pipelines for each layer."""

    schemas = ("bronze_layer", "silver_layer", "gold_layer")
    bronze_layer = (
        "grouped-daily-pipeline",
        "ticker-basic-details-pipeline",
        "sp500-basic-details-pipeline",
        "indexes-daily-close-pipeline",
        "financials-pipeline",
    )
    # Placeholder
    silver_layer = ()
    gold_layer = ()

    table_pipeline_mapping = {
        "bronze_layer": {
            "grouped_daily": "grouped-daily-pipeline",
            "ticker_basic_details": "ticker-basic-details-pipeline",
            "sp500_basic_details": "sp500-basic-details-pipeline",
            "indexes_daily_close": "indexes-daily-close-pipeline",
            "financials_balance_sheet": "financials-pipeline",
            "financials_cash_flow_statement": "financials-pipeline",
            "financials_income_statement": "financials-pipeline",
            "financials_comprehensive_income": "financials-pipeline",
        },
        "silver_layer": {},
        "gold_layer": {}
    }

    @classmethod
    def get_all_pipelines(cls) -> list[str]:
        """Get all pipelines for all layers."""
        return cls.bronze_layer + cls.silver_layer + cls.gold_layer  

    @classmethod
    def get_all_tables(cls):
        """Get all tables for all layers."""
        all_tables = []
        for layer in cls.table_pipeline_mapping.values():
            all_tables += layer.keys()
        return all_tables

    @classmethod
    def get_layer(cls, obj: str) -> list[str]:
        """Get pipelines for a specific layer."""
        layers = {
            "bronze_layer": cls.bronze_layer,
            "silver_layer": cls.silver_layer,
            "gold_layer": cls.gold_layer,
        }

        return layers.get(obj.lower(), [])

    @classmethod
    def get_pipeline_from_table(cls, table_name: str) -> str:
        """Get pipeline name for specific table."""

        all_layers_table_pipeline_mapping = {}
        for layer in cls.table_pipeline_mapping.values():
            all_layers_table_pipeline_mapping.update(layer)

        pipeline = all_layers_table_pipeline_mapping.get(table_name)
        if not pipeline:
            raise ValueError(f"Pipeline not found for table: {table_name}.")
        
        return pipeline


PIPELINES = Pipelines()

AVAILABLE_CLIENTS = ["SQLITE", "POSTGRES"]
SCOPE_HELP_TEXT = (
    "Defines the update scope. Use 'table' to update a specific table "
    "or 'schema' to update all tables under the schema. To update all tables, use 'all'."
    )
SUB_SCOPE_HELP_TEXT = (
    "Specifies the sub-scope to update: Name of the table if scope = 'pipeline'; "
    "name of schema if scope = 'schema'; not required if scope = 'all'."
    )
# Standard library
import json

# First party
from src.utils.constants import PIPELINES


def test_database_config():
    """Test database_config.json() file ."""

    def assert_table_config(table: dict):
        assert all(key in table.keys() for key in ["schema", "name", "fields_mapping"])
        assert table["schema"] in PIPELINES.schemas

    with open("src/database_config.json", "r") as file:
        tables_config = json.load(file)

    for pipeline, table in tables_config.items():
        if isinstance(table, dict):
            assert_table_config(table)
        else:
            for sub_table in table:
                assert_table_config(sub_table)

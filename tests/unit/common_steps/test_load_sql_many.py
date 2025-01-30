# Standard library
from unittest.mock import patch

# First party
from src.clients.sqlite_client import SQLiteClient
from src.common_steps.load_sql_many import SQLManyLoader
from src.settings import Settings
from tests.unit.mock_objects.mock_steps import MockSQLLoader


@patch("src.common_steps.load_sql_many.SQLLoader", MockSQLLoader)
def test_load_sql_many():
    """Test SQLManyLoader."""

    # Arrange
    settings = Settings("financials-pipeline")
    client = SQLiteClient(settings)
    previous_output = {
        "SP500_FINANCIALS_BALANCE_SHEET": {
            "valid_file_path": "mock_valid_file_path",
            "invalid_file_path": "mock_invalid_file_path",
        },
        "SP500_FINANCIALS_CASH_FLOW": {
            "valid_file_path": "mock_valid_file_path",
            "invalid_file_path": "mock_invalid_file_path",
        },
        "SP500_FINANCIALS_INCOME": {
            "valid_file_path": "mock_valid_file_path",
            "invalid_file_path": "mock_invalid_file_path",
        },
        "SP500_FINANCIALS_COMPREHENSIVE_INCOME": {
            "valid_file_path": "mock_valid_file_path",
            "invalid_file_path": "mock_invalid_file_path",
        },
    }

    # Act
    sql_many_loader = SQLManyLoader(previous_output, settings, client)
    is_success, output = sql_many_loader.run()

    # Assert
    assert is_success

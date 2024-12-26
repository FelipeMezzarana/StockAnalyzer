# Standard library
from unittest.mock import patch

# First party
from src.common_steps.validate_many import ManyValidator
from src.settings import Settings
from tests.unit.mock_objects.mock_steps import MockValidator


@patch("src.common_steps.validate_many.Validator", MockValidator)
def test_load_sql_many():
    """Test SQLManyLoader."""

    # Arrange
    settings = Settings("financials-pipeline")
    previous_output = {
        "files_path": {
            "FINANCIALS_BALANCE_SHEET": {
                "valid_file_path": "mock_valid_file_path",
                "invalid_file_path": "mock_invalid_file_path",
            },
            "FINANCIALS_CASH_FLOW_STATEMENT": {
                "valid_file_path": "mock_valid_file_path",
                "invalid_file_path": "mock_invalid_file_path",
            },
            "FINANCIALS_INCOME_STATEMENT": {
                "valid_file_path": "mock_valid_file_path",
                "invalid_file_path": "mock_invalid_file_path",
            },
            "FINANCIALS_COMPREHENSIVE_INCOME": {
                "valid_file_path": "mock_valid_file_path",
                "invalid_file_path": "mock_invalid_file_path",
            },
        }
    }

    # Act
    many_validator = ManyValidator(previous_output, settings)
    is_success, output = many_validator.run()

    # Assert
    assert is_success

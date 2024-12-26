# Standard library
from datetime import datetime
from unittest.mock import patch
import pytest

# First party
from src.pipelines.financials.steps.check_financials_tables import FinancialsChecker
from src.settings import Settings
from tests.unit.mock_objects.mock_clients import MockSQLHandler
from tests.utils.test_case_loader import TestCaseLoader

get_last_update_test_cases = TestCaseLoader("unit/pipelines/financials/get_last_update").load()


@pytest.mark.parametrize(*get_last_update_test_cases)
@patch("src.pipelines.financials.steps.check_financials_tables.SQLHandler")
def test_get_last_update(
    mock_sql_handler,
    required_tickers: list[str],
    expected_query_results: list,
    expected_result: dict,
):
    """Test FinancialsChecker.test_get_last_update."""

    # Arrange
    # json: list[list[str, str]] -> query results: list[tuple[str, datetime]]
    formatted_expected_query_results = [
        [(row[0], datetime.strptime(row[1], "%Y-%m-%d")) for row in query_result]
        for query_result in expected_query_results
    ]
    mock_sql_handler.return_value = MockSQLHandler(formatted_expected_query_results)
    settings = Settings("financials-pipeline")
    financials_checker = FinancialsChecker({}, settings, None)
    # Act
    next_update = financials_checker.get_last_update(required_tickers)
    # Assert
    assert next_update == expected_result


@patch("src.pipelines.financials.steps.check_financials_tables.SQLHandler")
def test_get_required_tickers(mock_sql_handler):
    """Test SQLManyLoader."""

    # Arrange
    expected_query_results = [
        [
            ("TICKER_1",),
            ("TICKER_2",),
            ("TICKER_3",),
        ]
    ]
    mock_sql_handler.return_value = MockSQLHandler(expected_query_results)
    settings = Settings("financials-pipeline")
    financials_checker = FinancialsChecker({}, settings, None)
    # Act
    tickers = financials_checker.get_required_tickers()
    # Assert
    assert tickers == ["TICKER_1", "TICKER_2", "TICKER_3"]

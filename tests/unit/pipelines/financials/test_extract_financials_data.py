# Standard library
from unittest.mock import patch

# First party
from src.pipelines.financials.steps.extract_financials_data import FinancialsExtractor
from src.settings import Settings
from tests.unit.mock_objects.mock_clients import MockPolygon
from tests.utils.test_case_loader import TestCaseLoader

get_last_update_test_cases = TestCaseLoader("unit/pipelines/financials/get_last_update").load()


@patch("src.pipelines.financials.steps.extract_financials_data.Polygon")
def test_request_ticker(mock_polygon):
    """Test FinancialsExtractor.request_ticker."""

    # Arrange
    mock_polygon.return_value = MockPolygon({"RESULTS": "MOCK_RESULTS"})
    settings = Settings("financials-pipeline")
    financials_extractor = FinancialsExtractor({}, settings)
    # Act
    response = financials_extractor.request_ticker("TICKER_NAME")
    # Assert
    assert response == {"RESULTS": "MOCK_RESULTS"}


@patch("src.pipelines.financials.steps.extract_financials_data.Polygon", MockPolygon)
def test_filter_results():
    """Test FinancialsExtractor.filter_results."""
    # Arrange
    settings = Settings("financials-pipeline")
    previous_output = {
        "required_tickers": {"TICKER_NAME": {"next_date": "2024-09-31", "last_date": "2024-06-01"}}
    }
    financials_extractor = FinancialsExtractor(previous_output, settings)
    data = [
        {"end_date": "2024-12-01", "data": "Mock data"},
        {"end_date": "2024-08-01", "data": "Mock data"},
        {"end_date": "2024-06-01", "data": "Mock data"},
        {"end_date": "2024-05-01", "data": "Mock data"},
    ]
    # Act
    response = financials_extractor.filter_results(data, "TICKER_NAME")
    # Assert
    assert response == [
        {"end_date": "2024-12-01", "data": "Mock data"},
        {"end_date": "2024-08-01", "data": "Mock data"},
    ]


@patch("src.pipelines.financials.steps.extract_financials_data.Polygon", MockPolygon)
def test_enrich_results():
    """Test FinancialsExtractor.enrich_results."""
    # Arrange
    settings = Settings("financials-pipeline")
    previous_output = {
        "required_tickers": {"TICKER_NAME": {"next_date": "2024-09-31", "last_date": "2024-06-01"}}
    }
    financials_extractor = FinancialsExtractor(previous_output, settings)
    data = [
        {"end_date": "2024-12-01", "data": "Mock data"},
        {"end_date": "2024-08-01", "data": "Mock data"},
    ]
    # Act
    response = financials_extractor.enrich_results(data, "TICKER_NAME")
    for item in response:  # Update date = datetime.now()
        item["updated_at"] = "mocked_date"
    # Assert
    assert response == [
        {
            "end_date": "2024-12-01",
            "data": "Mock data",
            "exchange_symbol_search": "TICKER_NAME",
            "updated_at": "mocked_date",
        },
        {
            "end_date": "2024-08-01",
            "data": "Mock data",
            "exchange_symbol_search": "TICKER_NAME",
            "updated_at": "mocked_date",
        },
    ]


@patch("src.pipelines.financials.steps.extract_financials_data.Polygon", MockPolygon)
def test_map_to_file():
    """Test FinancialsExtractor.map_to_file."""
    # Arrange
    settings = Settings("financials-pipeline")
    financials_extractor = FinancialsExtractor({}, settings)
    financials_extractor.output["files_path"] = {"FINANCIALS_COMPREHENSIVE_INCOME": "mocked_path"}
    data = [
        {
            "start_date": "2024-09-01",
            "end_date": "2024-12-01",
            "timeframe": "quarterly",
            "financials": {
                "comprehensive_income": {
                    "comprehensive_income_loss": {"value": 0},
                }
            },
        }
    ]
    # Act
    result = financials_extractor.map_to_file(data, "FINANCIALS_COMPREHENSIVE_INCOME")
    expected_result = [
        {
            "start_date": "2024-09-01",
            "end_date": "2024-12-01",
            "timeframe": "quarterly",
            "fiscal_period": None,
            "fiscal_year": None,
            "exchange_symbol_search": None,
            "tickers": None,
            "company_name": None,
            "updated_at": None,
            "comprehensive_income_loss": 0,
            "comprehensive_income_loss_attributable_to_noncontrolling_interest": None,
            "comprehensive_income_loss_attributable_to_parent": None,
            "other_comprehensive_income_loss": None,
            "other_comprehensive_income_loss_attributable_to_noncontrolling_interest": None,
            "other_comprehensive_income_loss_attributable_to_parent": None,
        }
    ]
    # Assert
    assert result == expected_result

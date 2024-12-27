# Standard library
import logging
import os

# Third party
import pytest

# First party
from src.clients.sqlite_client import SQLiteClient
from src.factories.pipeline_factory import PipelineFactory
from src.settings import Settings
from src.utils.sql_handler import SQLHandler
from tests.utils.test_case_loader import TestCaseLoader

test_cases = TestCaseLoader("integration/integration").load()
logger = logging.getLogger(__name__)


@pytest.mark.parametrize(*test_cases)
def test_integration(
    pipeline: str,
    multiple_tables: bool,
) -> None:
    """Test integration of pipelines with SQLite client.
    Run each pipeline mocking a few settings to load less data.
    Check if data is stored in the database.
    """
    # Arrange
    logger.info(f"Setting up integration test for {pipeline} pipeline")
    os.environ["CLIENT"] = "SQLITE"
    settings = Settings(pipeline)
    # Create new DB for tests
    settings.CLIENT_CONFIG["DB_PATH"] = "database/stock_database_test.db"
    client = SQLiteClient(settings)
    # Mock few settings to limit data extracted
    settings.is_integration_test = True
    settings.POLYGON["MAX_PAGINATION"] = 2
    settings.POLYGON["POLYGON_MAX_DAYS_HIST"] = 4
    settings.FRED["INDEXES"] = ["SP500"]

    # Act
    logger.info("Running pipeline")
    pipeline = PipelineFactory(settings).create()
    is_successful = pipeline.run()

    # Assert
    logger.info("Asserting results")
    assert is_successful
    # Check db
    if multiple_tables:
        for table in settings.PIPELINE_TABLE:
            sqlite_handler = SQLHandler(settings, client, table)
            table_name = table["schema"] + "." + table["name"]
            is_successful, rows_qty = sqlite_handler.query(f"SELECT COUNT(*) FROM {table_name}")
            assert is_successful
            assert rows_qty[0][0] > 10
    else:
        sqlite_handler = SQLHandler(settings, client)
        table_name = settings.PIPELINE_TABLE["schema"] + "." + settings.PIPELINE_TABLE["name"]
        is_successful, rows_qty = sqlite_handler.query(f"SELECT COUNT(*) FROM {table_name}")
        assert is_successful
        assert rows_qty[0][0] > 10  # Check if we have some data

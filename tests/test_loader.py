from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from config.settings import DatabaseConfig
from loaders.warehouse_loader import WarehouseLoader


@pytest.fixture
def mock_engine():
    """Create a mock engine for testing"""
    engine = Mock()
    return engine


@pytest.fixture
def mock_session():
    """Create a mock session for testing"""
    session = Mock()
    session.commit = Mock()
    session.rollback = Mock()
    session.close = Mock()
    return session


def test_warehouse_loader_initialization():
    """Test WarehouseLoader initialization"""
    db_config = DatabaseConfig()
    loader = WarehouseLoader(db_config)
    assert loader.db_config == db_config


def test_warehouse_loader_get_session(mock_engine):
    """Test WarehouseLoader session management"""
    db_config = DatabaseConfig()
    with patch("sqlalchemy.create_engine", return_value=mock_engine):
        loader = WarehouseLoader(db_config)

        # Mock SessionLocal
        mock_session_local = Mock()
        mock_session = Mock()
        mock_session_local.return_value = mock_session
        loader.SessionLocal = mock_session_local

        with loader.get_session() as session:
            assert session == mock_session


def test_warehouse_loader_bulk_insert_crypto_prices_empty():
    """Test bulk insert with empty data"""
    db_config = DatabaseConfig()
    loader = WarehouseLoader(db_config)

    # Mock engine and pandas
    loader.engine = Mock()
    with patch("pandas.DataFrame") as mock_df:
        mock_df_instance = Mock()
        mock_df.return_value = mock_df_instance

        result = loader.bulk_insert_crypto_prices([])
        assert result == 0


def test_warehouse_loader_bulk_insert_crypto_prices_success():
    """Test successful bulk insert"""
    db_config = DatabaseConfig()
    loader = WarehouseLoader(db_config)

    # Mock engine and pandas
    loader.engine = Mock()
    with patch("pandas.DataFrame") as mock_df:
        mock_df_instance = Mock()
        mock_df.return_value = mock_df_instance

        test_data = [
            {"symbol": "BTC", "current_price": 50000.0},
            {"symbol": "ETH", "current_price": 3000.0},
        ]

        result = loader.bulk_insert_crypto_prices(test_data)
        assert result == 2


def test_warehouse_loader_bulk_insert_crypto_prices_error():
    """Test bulk insert with database error"""
    db_config = DatabaseConfig()
    loader = WarehouseLoader(db_config)

    # Mock engine to raise an exception
    loader.engine = Mock()
    loader.engine.to_sql.side_effect = Exception("Database error")

    with patch("pandas.DataFrame"):
        test_data = [{"symbol": "BTC", "current_price": 50000.0}]

        with pytest.raises(Exception, match="Database error"):
            loader.bulk_insert_crypto_prices(test_data)


def test_warehouse_loader_log_pipeline_run():
    """Test logging pipeline run"""
    db_config = DatabaseConfig()
    loader = WarehouseLoader(db_config)

    # Mock session
    mock_session = Mock()
    loader.get_session = Mock()
    loader.get_session.return_value.__enter__.return_value = mock_session

    # Mock insert statement
    with patch("sqlalchemy.dialects.postgresql.insert") as mock_insert:
        mock_stmt = Mock()
        mock_insert.return_value.values.return_value.on_conflict_do_update.return_value = (
            mock_stmt
        )

        loader.log_pipeline_run(
            run_id="test_run", stage="extract", status="success", records_processed=100
        )

        # Verify the session execute was called
        mock_session.execute.assert_called_once()

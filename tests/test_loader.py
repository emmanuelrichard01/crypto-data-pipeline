from unittest.mock import MagicMock, Mock, patch
import pytest

from config.settings import DatabaseConfig
from loaders.warehouse_loader import WarehouseLoader


def test_warehouse_loader_initialization():
    db_config = DatabaseConfig()
    loader = WarehouseLoader(db_config)
    assert loader.db_config == db_config


def test_warehouse_loader_get_session():
    db_config = DatabaseConfig()
    loader = WarehouseLoader(db_config)
    mock_session = MagicMock()
    loader.SessionLocal = Mock(return_value=mock_session)
    with loader.get_session() as session:
        assert session == mock_session


def test_warehouse_loader_bulk_insert_crypto_prices_empty():
    db_config = DatabaseConfig()
    loader = WarehouseLoader(db_config)
    with patch("pandas.DataFrame") as mock_df:
        mock_df_instance = Mock()
        mock_df.return_value = mock_df_instance
        result = loader.bulk_insert_crypto_prices([])
        assert result == 0


def test_warehouse_loader_bulk_insert_crypto_prices_success():
    db_config = DatabaseConfig()
    loader = WarehouseLoader(db_config)
    with patch("pandas.DataFrame") as mock_df:
        mock_df_instance = Mock()
        mock_df.return_value = mock_df_instance
        test_data = [{"symbol": "BTC", "current_price": 50000.0}]
        result = loader.bulk_insert_crypto_prices(test_data)
        assert result == 1


def test_warehouse_loader_bulk_insert_crypto_prices_error():
    db_config = DatabaseConfig()
    loader = WarehouseLoader(db_config)
    with patch("pandas.DataFrame.to_sql", side_effect=Exception("Database error")):
        with pytest.raises(Exception, match="Database error"):
            loader.bulk_insert_crypto_prices(
                [{"symbol": "BTC", "current_price": 50000.0}]
            )


def test_warehouse_loader_log_pipeline_run():
    db_config = DatabaseConfig()
    loader = WarehouseLoader(db_config)
    mock_session = MagicMock()
    loader.get_session = MagicMock()
    loader.get_session.return_value.__enter__.return_value = mock_session
    with patch("sqlalchemy.dialects.postgresql.insert") as mock_insert:
        mock_stmt = Mock()
        mock_insert.return_value.values.return_value.on_conflict_do_update.return_value = (
            mock_stmt
        )
        loader.log_pipeline_run("test_run", "extract", "success", 100)
        mock_session.execute.assert_called_once()

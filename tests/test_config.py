import os
from unittest.mock import patch
import pytest

from config.settings import DatabaseConfig, PipelineConfig


def test_database_config_defaults():
    config = DatabaseConfig()
    assert config.host == "localhost"
    assert config.port == 5432
    assert config.database == "crypto_warehouse"
    assert config.user == "postgres"
    assert config.password == "crypto_password_123"
    assert config.batch_size == 100


def test_database_config_from_env():
    with patch.dict(
        os.environ,
        {
            "DB_HOST": "test_host",
            "DB_PORT": "5433",
            "DB_NAME": "test_db",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_password",
            "BATCH_SIZE": "200",
        },
        clear=True,
    ):
        config = DatabaseConfig()
        assert config.host == "test_host"
        assert config.port == 5433
        assert config.database == "test_db"
        assert config.user == "test_user"
        assert config.password == "test_password"
        assert config.batch_size == 200


def test_database_config_validation():
    with patch.dict(os.environ, {"DB_PORT": "0"}, clear=True):
        with pytest.raises(ValueError, match="DB_PORT must be between 1 and 65535"):
            DatabaseConfig()

    with patch.dict(os.environ, {"BATCH_SIZE": "-1"}, clear=True):
        with pytest.raises(ValueError, match="BATCH_SIZE must be positive"):
            DatabaseConfig()


def test_pipeline_config_defaults():
    config = PipelineConfig()
    assert config.extraction_interval_minutes == 60
    assert config.batch_size == 100
    assert config.max_retries == 3
    assert config.timeout_seconds == 30
    assert config.cryptocurrencies == ["bitcoin", "ethereum", "cardano", "polkadot", "chainlink"]


def test_pipeline_config_from_env():
    with patch.dict(
        os.environ,
        {
            "EXTRACTION_INTERVAL_MINUTES": "30",
            "BATCH_SIZE": "150",
            "MAX_RETRIES": "5",
            "TIMEOUT_SECONDS": "60",
            "CRYPTOCURRENCIES": "bitcoin,ethereum",
        },
        clear=True,
    ):
        config = PipelineConfig()
        assert config.extraction_interval_minutes == 30
        assert config.batch_size == 150
        assert config.max_retries == 5
        assert config.timeout_seconds == 60
        assert config.cryptocurrencies == ["bitcoin", "ethereum"]


def test_pipeline_config_validation():
    with patch.dict(os.environ, {"EXTRACTION_INTERVAL_MINUTES": "0"}, clear=True):
        with pytest.raises(ValueError, match="EXTRACTION_INTERVAL_MINUTES must be positive"):
            PipelineConfig()

    with patch.dict(os.environ, {"CRYPTOCURRENCIES": ""}, clear=True):
        with pytest.raises(ValueError, match="CRYPTOCURRENCIES list cannot be empty"):
            PipelineConfig()

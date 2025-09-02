import pytest
import asyncio
from unittest.mock import AsyncMock, Mock, patch
from pipeline.orchestrator import CryptoPipelineOrchestrator
from config.settings import PipelineConfig, DatabaseConfig


@pytest.mark.asyncio
async def test_orchestrator_runs():
    orchestrator = CryptoPipelineOrchestrator(PipelineConfig(), DatabaseConfig())
    result = await orchestrator.run_extraction_pipeline()
    assert "run_id" in result


@pytest.mark.asyncio
async def test_orchestrator_handles_extraction_error():
    # Mock the extractor to raise an exception
    with patch("extractors.crypto_extractor.CryptoDataExtractor") as mock_extractor:
        mock_extractor_instance = AsyncMock()
        mock_extractor_instance.__aenter__.return_value = mock_extractor_instance
        mock_extractor_instance.fetch_crypto_prices.side_effect = Exception("API Error")
        mock_extractor.return_value = mock_extractor_instance

        # Mock the loader
        with patch("loaders.warehouse_loader.WarehouseLoader") as mock_loader:
            mock_loader_instance = Mock()
            mock_loader_instance.log_pipeline_run = Mock()
            mock_loader.return_value = mock_loader_instance

            orchestrator = CryptoPipelineOrchestrator(
                PipelineConfig(), DatabaseConfig()
            )
            # Replace the loader with our mock
            orchestrator.loader = mock_loader_instance

            result = await orchestrator.run_extraction_pipeline()
            assert result["status"] == "failed"
            assert "error" in result


@pytest.mark.asyncio
async def test_orchestrator_handles_loading_error():
    # Mock the extractor to return data
    with patch("extractors.crypto_extractor.CryptoDataExtractor") as mock_extractor:
        mock_extractor_instance = AsyncMock()
        mock_extractor_instance.__aenter__.return_value = mock_extractor_instance
        mock_extractor_instance.fetch_crypto_prices.return_value = [
            {"symbol": "BTC", "price": 10000}
        ]
        mock_extractor.return_value = mock_extractor_instance

        # Mock the loader to raise an exception
        with patch("loaders.warehouse_loader.WarehouseLoader") as mock_loader:
            mock_loader_instance = Mock()
            mock_loader_instance.log_pipeline_run = Mock()
            mock_loader_instance.bulk_insert_crypto_prices.side_effect = Exception(
                "Database Error"
            )
            mock_loader.return_value = mock_loader_instance

            orchestrator = CryptoPipelineOrchestrator(
                PipelineConfig(), DatabaseConfig()
            )
            # Replace the loader with our mock
            orchestrator.loader = mock_loader_instance

            result = await orchestrator.run_extraction_pipeline()
            assert result["status"] == "failed"
            assert "error" in result

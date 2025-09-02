import pytest
from unittest.mock import AsyncMock, patch

from config.settings import PipelineConfig
from extractors.crypto_extractor import CryptoDataExtractor


@pytest.mark.asyncio
async def test_extractor_fetches_data():
    config = PipelineConfig(cryptocurrencies=["bitcoin"])
    with patch("aiohttp.ClientSession.get") as mock_get:
        mock_response = AsyncMock()
        mock_response.json.return_value = [{"symbol": "BTC", "price": 10000}]
        mock_response.status = 200
        mock_get.return_value.__aenter__.return_value = mock_response

        async with CryptoDataExtractor(config) as extractor:
            data = await extractor.fetch_crypto_prices()
            assert isinstance(data, list)
            assert "symbol" in data[0]


@pytest.mark.asyncio
async def test_extractor_handles_empty_response():
    config = PipelineConfig(cryptocurrencies=["bitcoin"])
    with patch("aiohttp.ClientSession.get") as mock_get:
        mock_response = AsyncMock()
        mock_response.json.return_value = []
        mock_response.status = 200
        mock_get.return_value.__aenter__.return_value = mock_response

        async with CryptoDataExtractor(config) as extractor:
            data = await extractor.fetch_crypto_prices()
            assert isinstance(data, list)
            assert len(data) == 0


@pytest.mark.asyncio
async def test_extractor_handles_api_error():
    config = PipelineConfig(cryptocurrencies=["bitcoin"])
    with patch("aiohttp.ClientSession.get") as mock_get:
        mock_response = AsyncMock()
        mock_response.text.return_value = "API Error"
        mock_response.status = 500
        mock_get.return_value.__aenter__.return_value = mock_response

        async with CryptoDataExtractor(config) as extractor:
            with pytest.raises(Exception):
                await extractor.fetch_crypto_prices()

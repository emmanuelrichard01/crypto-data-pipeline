import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiohttp
import backoff

from src.config.settings import PipelineConfig
from src.extractors.secrets import get_coingecko_api_key  # 🔑 secure import

logger = logging.getLogger(__name__)


class CryptoDataExtractor:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.base_url = "https://api.coingecko.com/api/v3"
        self.api_key = get_coingecko_api_key()  # 🔐 secure fetch
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=3,
        max_time=60,
    )
    async def fetch_crypto_prices(self) -> List[Dict]:
        if not self.session:
            raise RuntimeError("Extractor session not initialized")

        crypto_ids = ",".join(self.config.cryptocurrencies)
        url = f"{self.base_url}/coins/markets"

        headers = {
            "accept": "application/json",
            "x-cg-demo-api-key": self.api_key,  # ✅ secured
        }

        params = {
            "vs_currency": "usd",
            "ids": crypto_ids,
            "order": "market_cap_desc",
            "per_page": len(self.config.cryptocurrencies),
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "1h,24h,7d",
        }

        logger.info(f"Fetching data for: {crypto_ids}")

        async with self.session.get(url, headers=headers, params=params) as response:
            if response.status == 429:
                logger.warning("Rate limit hit. Retrying after cooldown...")
                await asyncio.sleep(60)
                raise aiohttp.ClientError(f"Rate limit exceeded: {response.status}")
            elif response.status in (401, 403):
                error_msg = await response.text()
                logger.error(f"API Auth Error {response.status}: {error_msg}")
                raise aiohttp.ClientError(
                    f"API key issue ({response.status}): {error_msg}"
                )
            elif response.status != 200:
                error_msg = await response.text()
                logger.error(f"API Error {response.status}: {error_msg}")
                raise aiohttp.ClientError(f"Error {response.status}: {error_msg}")

            data = await response.json()
            extraction_time = datetime.utcnow()

            return [
                {
                    "symbol": coin.get("symbol", "").upper(),
                    "name": coin.get("name", ""),
                    "current_price": coin.get("current_price", 0.0),
                    "market_cap": coin.get("market_cap"),
                    "total_volume": coin.get("total_volume"),
                    "price_change_24h": coin.get("price_change_24h"),
                    "price_change_percentage_24h": coin.get("price_change_percentage_24h"),
                    "price_change_percentage_1h": coin.get("price_change_percentage_1h_in_currency"),
                    "price_change_percentage_7d": coin.get("price_change_percentage_7d_in_currency"),
                    "market_cap_rank": coin.get("market_cap_rank"),
                    "circulating_supply": coin.get("circulating_supply"),
                    "total_supply": coin.get("total_supply"),
                    "max_supply": coin.get("max_supply"),
                    "ath": coin.get("ath"),
                    "atl": coin.get("atl"),
                    "last_updated": coin.get("last_updated"),
                    "extracted_at": extraction_time,
                }
                for coin in data
            ]

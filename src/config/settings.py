import os
from dataclasses import dataclass
from typing import List, Dict, Optional
import json
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

@dataclass
class DatabaseConfig:
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", "5432"))
    database: str = os.getenv("DB_NAME", "crypto_warehouse")
    user: str = os.getenv("DB_USER", "postgres")
    password: str = os.getenv("DB_PASSWORD", "crypto_password_123")
    batch_size: int = int(os.getenv("BATCH_SIZE", "100"))
    
    def __post_init__(self):
        # Validate required fields
        if not self.host:
            raise ValueError("DB_HOST is required")
        if not self.database:
            raise ValueError("DB_NAME is required")
        if not self.user:
            raise ValueError("DB_USER is required")
        if not self.password:
            raise ValueError("DB_PASSWORD is required")
        if not (1 <= self.port <= 65535):
            raise ValueError("DB_PORT must be between 1 and 65535")
        if self.batch_size <= 0:
            raise ValueError("BATCH_SIZE must be positive")
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class PipelineConfig:
    cryptocurrencies: List[str] = None
    extraction_interval_minutes: int = int(os.getenv("EXTRACTION_INTERVAL_MINUTES", "60"))
    batch_size: int = int(os.getenv("BATCH_SIZE", "100"))
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    timeout_seconds: int = int(os.getenv("TIMEOUT_SECONDS", "30"))
    
    def __post_init__(self):
        # Get cryptocurrencies from environment variable or use default
        crypto_env = os.getenv("CRYPTOCURRENCIES")
        if crypto_env:
            self.cryptocurrencies = [crypto.strip() for crypto in crypto_env.split(",")]
        elif self.cryptocurrencies is None:
            self.cryptocurrencies = ["bitcoin", "ethereum", "cardano", "polkadot", "chainlink"]
        
        # Validate configuration parameters
        if self.extraction_interval_minutes <= 0:
            raise ValueError("EXTRACTION_INTERVAL_MINUTES must be positive")
        if self.batch_size <= 0:
            raise ValueError("BATCH_SIZE must be positive")
        if self.max_retries < 0:
            raise ValueError("MAX_RETRIES must be non-negative")
        if self.timeout_seconds <= 0:
            raise ValueError("TIMEOUT_SECONDS must be positive")
        if not self.cryptocurrencies:
            raise ValueError("CRYPTOCURRENCIES list cannot be empty")

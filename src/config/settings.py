import logging
import os
from dataclasses import dataclass, field
from typing import List, Optional

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    host: str = field(default_factory=lambda: os.environ["DB_HOST"])
    port: int = field(default_factory=lambda: int(os.environ["DB_PORT"]))
    database: str = field(default_factory=lambda: os.environ["DB_NAME"])
    user: str = field(default_factory=lambda: os.environ["DB_USER"])
    password: str = field(default_factory=lambda: os.environ["DB_PASSWORD"])
    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "100")))

    def __post_init__(self):
        if not (1 <= self.port <= 65535):
            raise ValueError("DB_PORT must be between 1 and 65535")
        if self.batch_size <= 0:
            raise ValueError("BATCH_SIZE must be positive")

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class PipelineConfig:
    extraction_interval_minutes: int = field(
        default_factory=lambda: int(os.getenv("EXTRACTION_INTERVAL_MINUTES", "60"))
    )
    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "100")))
    max_retries: int = field(default_factory=lambda: int(os.getenv("MAX_RETRIES", "3")))
    timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("TIMEOUT_SECONDS", "30"))
    )
    cryptocurrencies: Optional[List[str]] = field(default=None)

    def __post_init__(self):
        # Load cryptocurrencies from env or defaults
        crypto_env = os.getenv("CRYPTOCURRENCIES")
        if crypto_env is not None:
            self.cryptocurrencies = [
                c.strip() for c in crypto_env.split(",") if c.strip()
            ]
        elif self.cryptocurrencies is None:
            self.cryptocurrencies = [
                "bitcoin",
                "ethereum",
                "cardano",
                "polkadot",
                "chainlink",
            ]

        # Validation
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

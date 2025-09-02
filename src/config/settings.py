import logging
import os
from dataclasses import dataclass, field
from typing import List, Optional

from dotenv import load_dotenv

# Load environment variables from .env file (if present)
load_dotenv()
logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    batch_size: Optional[int] = None

    def __post_init__(self):
        # Lazy load values from env only if not provided directly
        self.host = self.host or os.getenv("DB_HOST", "localhost")
        self.port = self.port or int(os.getenv("DB_PORT", "5432"))
        self.database = self.database or os.getenv("DB_NAME", "crypto_warehouse")
        self.user = self.user or os.getenv("DB_USER", "postgres")
        self.password = self.password or os.getenv("DB_PASSWORD", "crypto_password_123")
        self.batch_size = self.batch_size or int(os.getenv("BATCH_SIZE", "100"))

        # Validation
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
        """Return a SQLAlchemy-compatible Postgres connection string."""
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
    cryptocurrencies: List[str] = field(default_factory=list)


def __post_init__(self):
    crypto_env = os.getenv("CRYPTOCURRENCIES")
    if crypto_env is not None:
        # Explicitly set but empty
        if not crypto_env.strip():
            raise ValueError("CRYPTOCURRENCIES list cannot be empty")
        self.cryptocurrencies = [c.strip() for c in crypto_env.split(",") if c.strip()]
    elif not self.cryptocurrencies:
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

import logging
import os
from dataclasses import dataclass, field
from typing import List

from dotenv import load_dotenv

# Load environment variables
load_dotenv()
logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    host: str = None
    port: int = None
    database: str = None
    user: str = None
    password: str = None
    batch_size: int = None

    def __post_init__(self):
        # Defaults
        self.host = (
            os.getenv("DB_HOST", "localhost") if self.host is None else self.host
        )
        self.port = (
            int(os.getenv("DB_PORT", "5432")) if self.port is None else self.port
        )
        self.database = (
            os.getenv("DB_NAME", "crypto_warehouse")
            if self.database is None
            else self.database
        )
        self.user = os.getenv("DB_USER", "postgres") if self.user is None else self.user
        self.password = (
            os.getenv("DB_PASSWORD", "crypto_password_123")
            if self.password is None
            else self.password
        )
        self.batch_size = (
            int(os.getenv("BATCH_SIZE", "100"))
            if self.batch_size is None
            else self.batch_size
        )

        # Validation
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
    cryptocurrencies: List[str] = field(default_factory=list)

    def __post_init__(self):
        # Validation first
        if self.extraction_interval_minutes <= 0:
            raise ValueError("EXTRACTION_INTERVAL_MINUTES must be positive")
        if self.batch_size <= 0:
            raise ValueError("BATCH_SIZE must be positive")
        if self.max_retries < 0:
            raise ValueError("MAX_RETRIES must be non-negative")
        if self.timeout_seconds <= 0:
            raise ValueError("TIMEOUT_SECONDS must be positive")

        # Handle CRYPTOCURRENCIES env
        crypto_env = os.getenv("CRYPTOCURRENCIES")
        if crypto_env is not None:
            if not crypto_env.strip():
                raise ValueError("CRYPTOCURRENCIES list cannot be empty")
            self.cryptocurrencies = [
                c.strip() for c in crypto_env.split(",") if c.strip()
            ]
        elif not self.cryptocurrencies:
            self.cryptocurrencies = [
                "bitcoin",
                "ethereum",
                "cardano",
                "polkadot",
                "chainlink",
            ]

        if not self.cryptocurrencies:
            raise ValueError("CRYPTOCURRENCIES list cannot be empty")

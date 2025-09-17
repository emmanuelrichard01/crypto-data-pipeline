"""Configuration management for the crypto data pipeline."""

import os
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class ConfigManager:
    """Manages configuration loading and validation."""

    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.config: Dict[str, Any] = {}

    def load_config(self) -> Dict[str, Any]:
        """Load configuration from various sources."""
        self.load_env_vars()
        self.load_profiles()
        self.validate_config()
        return self.config

    def load_env_vars(self) -> None:
        """Load configuration from environment variables."""
        self.config.update(
            {
                "database": {
                    "host": os.getenv(
                        "DB_HOST", "postgres"
                    ),  # Changed default to postgres for Docker
                    "port": int(os.getenv("DB_PORT", "5432")),
                    "name": os.getenv("DB_NAME", "crypto_warehouse"),
                    "user": os.getenv("DB_USER", "postgres"),
                    "password": os.getenv("DB_PASSWORD", "crypto_password_123"),
                    "batch_size": int(os.getenv("BATCH_SIZE", "100")),
                },
                "redis": {
                    "url": os.getenv(
                        "REDIS_URL", "redis://redis:6379"
                    ),  # Changed default to redis for Docker
                },
                "api": {
                    "coingecko_key": os.getenv("COINGECKO_API_KEY", ""),
                },
                "logging": {
                    "level": os.getenv("LOG_LEVEL", "INFO"),
                },
                "pipeline": {
                    "extraction_interval_minutes": int(
                        os.getenv("EXTRACTION_INTERVAL_MINUTES", "60")
                    ),
                    "batch_size": int(os.getenv("BATCH_SIZE", "100")),
                    "max_retries": int(os.getenv("MAX_RETRIES", "3")),
                    "timeout_seconds": int(os.getenv("TIMEOUT_SECONDS", "30")),
                    "cryptocurrencies": [
                        c.strip()
                        for c in os.getenv(
                            "CRYPTOCURRENCIES",
                            "bitcoin,ethereum,cardano,polkadot,chainlink",
                        ).split(",")
                        if c.strip()
                    ],
                },
            }
        )

    def load_profiles(self) -> None:
        """Load DBT profiles configuration."""
        profile_path = self.config_dir / "profiles.yml"
        if profile_path.exists():
            with open(profile_path) as f:
                self.config["dbt"] = yaml.safe_load(f)

    def validate_config(self) -> None:
        """Validate configuration values."""
        db = self.config["database"]
        if not (1 <= db["port"] <= 65535):
            raise ValueError("DB_PORT must be between 1 and 65535")
        if db["batch_size"] <= 0:
            raise ValueError("BATCH_SIZE must be positive")

        pipeline = self.config["pipeline"]
        if pipeline["extraction_interval_minutes"] <= 0:
            raise ValueError("EXTRACTION_INTERVAL_MINUTES must be positive")
        if pipeline["batch_size"] <= 0:
            raise ValueError("BATCH_SIZE must be positive")
        if pipeline["max_retries"] < 0:
            raise ValueError("MAX_RETRIES must be non-negative")
        if pipeline["timeout_seconds"] <= 0:
            raise ValueError("TIMEOUT_SECONDS must be positive")
        if not pipeline["cryptocurrencies"]:
            raise ValueError("CRYPTOCURRENCIES list cannot be empty")


config_manager = ConfigManager()
config = config_manager.load_config()

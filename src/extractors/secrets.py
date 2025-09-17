import os


def get_coingecko_api_key() -> str:
    """Get CoinGecko API key from environment variables."""
    api_key = os.getenv("COINGECKO_API_KEY")
    if not api_key:
        raise ValueError("❌ COINGECKO_API_KEY environment variable is required")
    return api_key

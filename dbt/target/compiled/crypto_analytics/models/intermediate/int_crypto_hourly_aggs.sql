

WITH base_data AS (
    SELECT
        symbol,
        crypto_name,
        DATE_TRUNC('hour', extracted_at) AS extraction_hour,
        price_usd,
        volume_24h,
        market_cap,
        CASE WHEN price_usd <= 0 THEN 1 ELSE 0 END AS is_invalid
    FROM "crypto_warehouse"."public_staging"."stg_crypto_prices"
)

SELECT
    symbol,
    crypto_name,
    extraction_hour,
    COUNT(*) AS total_records,
    SUM(is_invalid) AS invalid_price_records,
    AVG(price_usd) AS avg_price_usd,
    MIN(price_usd) AS min_price_usd,
    MAX(price_usd) AS max_price_usd,
    STDDEV(price_usd) AS price_volatility,
    AVG(volume_24h) AS avg_volume_24h,
    AVG(market_cap) AS avg_market_cap
FROM base_data
GROUP BY symbol, crypto_name, extraction_hour
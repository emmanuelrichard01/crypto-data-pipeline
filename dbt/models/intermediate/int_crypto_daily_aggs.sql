{{
  config(
    materialized='ephemeral'
  )
}}

SELECT
    symbol,
    crypto_name,
    extraction_date,

    -- OHLC data (Open, High, Low, Close)
    FIRST_VALUE(price_usd) OVER (
        PARTITION BY symbol, extraction_date
        ORDER BY extracted_at
        ROWS UNBOUNDED PRECEDING
    ) AS open_price,
    MAX(price_usd) AS high_price,
    MIN(price_usd) AS low_price,
    LAST_VALUE(price_usd) OVER (
        PARTITION BY symbol, extraction_date
        ORDER BY extracted_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS close_price,

    -- Volume and market metrics
    AVG(volume_24h) AS avg_volume_24h,
    AVG(market_cap) AS avg_market_cap,
    AVG(market_cap_rank) AS avg_market_cap_rank,

    -- Volatility measures
    STDDEV(price_usd) AS daily_volatility,
    (MAX(price_usd) - MIN(price_usd)) / AVG(price_usd) * 100 AS daily_range_pct,

    -- Data quality
    COUNT(*) AS total_records,
    MIN(extracted_at) AS first_extraction,
    MAX(extracted_at) AS last_extraction

FROM {{ ref('stg_crypto_prices') }}
GROUP BY symbol, crypto_name, extraction_date

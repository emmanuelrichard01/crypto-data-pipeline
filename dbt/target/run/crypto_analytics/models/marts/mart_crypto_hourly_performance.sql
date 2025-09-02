



  create  table "crypto_warehouse"."public_marts"."mart_crypto_hourly_performance__dbt_tmp"


    as

  (


WITH  __dbt__cte__int_crypto_hourly_aggs as (


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
), hourly_data AS (
    SELECT
        symbol,
        crypto_name,
        extraction_hour,
        avg_price_usd,
        min_price_usd,
        max_price_usd,
        price_volatility,
        avg_volume_24h,
        avg_market_cap,
        total_records,

        -- Calculate hourly change
        LAG(avg_price_usd) OVER (
            PARTITION BY symbol
            ORDER BY extraction_hour
        ) as previous_hour_price,

        -- Data quality score
        (1.0 - (invalid_price_records::FLOAT / GREATEST(total_records, 1))) * 100 as data_quality_score

    FROM __dbt__cte__int_crypto_hourly_aggs
),

performance_metrics AS (
    SELECT
        *,
        CASE
            WHEN previous_hour_price IS NOT NULL AND previous_hour_price > 0 THEN
                ((avg_price_usd - previous_hour_price) / previous_hour_price) * 100
            ELSE NULL
        END as hourly_change_pct,

        -- Volatility category
        CASE
            WHEN price_volatility IS NULL THEN 'Unknown'
            WHEN price_volatility < avg_price_usd * 0.01 THEN 'Low Volatility'
            WHEN price_volatility < avg_price_usd * 0.05 THEN 'Medium Volatility'
            ELSE 'High Volatility'
        END as volatility_category

    FROM hourly_data
)

SELECT * FROM performance_metrics
  );

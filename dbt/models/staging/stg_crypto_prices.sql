{{ config(
    materialized='table',
    post_hook="CREATE INDEX IF NOT EXISTS idx_stg_crypto_prices_symbol_time ON {{ this }} (symbol, extracted_at)"
) }}

WITH source_data AS (
    SELECT
        id AS price_id,
        current_price AS price_usd,
        market_cap,
        total_volume AS volume_24h,
        price_change_24h,
        price_change_percentage_24h AS price_change_pct_24h,
        market_cap_rank,
        extracted_at,
        created_at,
        UPPER(TRIM(symbol)) AS symbol,
        TRIM(name) AS crypto_name,

        COALESCE(current_price <= 0, FALSE) AS is_invalid_price,
        COALESCE(market_cap <= 0, FALSE) AS is_invalid_market_cap,
        COALESCE(total_volume < 0, FALSE) AS is_invalid_volume
    FROM {{ source('raw', 'crypto_prices_raw') }}
),

cleaned_data AS (
    SELECT
        *,
        DATE(extracted_at) AS extraction_date,
        DATE_TRUNC('hour', extracted_at) AS extraction_hour,
        CASE
            WHEN LAG(price_usd) OVER (
                PARTITION BY symbol ORDER BY extracted_at
            ) IS NOT NULL THEN
                ((price_usd - LAG(price_usd) OVER (
                    PARTITION BY symbol ORDER BY extracted_at
                )) / LAG(price_usd) OVER (
                    PARTITION BY symbol ORDER BY extracted_at
                )) * 100
        END AS price_change_pct_from_previous
    FROM source_data
    WHERE NOT is_invalid_price AND NOT is_invalid_volume
)

SELECT * FROM cleaned_data

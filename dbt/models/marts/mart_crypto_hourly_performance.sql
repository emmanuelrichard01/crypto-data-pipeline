{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['symbol', 'extraction_hour'], 'type': 'btree'},
      {'columns': ['extraction_hour'], 'type': 'btree'}
    ]
  )
}}

WITH hourly_data AS (
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
        
    FROM {{ ref('int_crypto_hourly_aggs') }}
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
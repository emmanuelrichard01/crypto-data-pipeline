

WITH source_data AS (
    SELECT 
        id as price_id,
        UPPER(TRIM(symbol)) as symbol,
        TRIM(name) as crypto_name,
        current_price as price_usd,
        market_cap,
        total_volume as volume_24h,
        price_change_24h,
        price_change_percentage_24h as price_change_pct_24h,
        market_cap_rank,
        extracted_at,
        created_at,
        
        CASE WHEN current_price <= 0 THEN TRUE ELSE FALSE END as is_invalid_price,
        CASE WHEN market_cap <= 0 THEN TRUE ELSE FALSE END as is_invalid_market_cap,
        CASE WHEN total_volume < 0 THEN TRUE ELSE FALSE END as is_invalid_volume
    FROM "crypto_warehouse"."public"."crypto_prices_raw"
),

cleaned_data AS (
    SELECT 
        *,
        DATE(extracted_at) as extraction_date,
        DATE_TRUNC('hour', extracted_at) as extraction_hour,
        CASE 
            WHEN LAG(price_usd) OVER (
                PARTITION BY symbol ORDER BY extracted_at
            ) IS NOT NULL THEN
                ((price_usd - LAG(price_usd) OVER (
                    PARTITION BY symbol ORDER BY extracted_at
                )) / LAG(price_usd) OVER (
                    PARTITION BY symbol ORDER BY extracted_at
                )) * 100
        END as price_change_pct_from_previous
    FROM source_data
    WHERE NOT is_invalid_price AND NOT is_invalid_volume
)

SELECT * FROM cleaned_data
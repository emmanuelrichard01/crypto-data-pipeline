
  
    

  create  table "crypto_warehouse"."public_marts"."mart_crypto_latest_prices__dbt_tmp"
  
  
    as
  
  (
    

WITH latest_prices AS (
    SELECT 
        symbol,
        crypto_name,
        price_usd,
        market_cap,
        volume_24h,
        price_change_24h,
        price_change_pct_24h,
        market_cap_rank,
        extracted_at,
        
        ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY extracted_at DESC
        ) as rn
        
    FROM "crypto_warehouse"."public_staging"."stg_crypto_prices"
    WHERE extraction_date = CURRENT_DATE
),

price_changes AS (
    SELECT 
        symbol,
        crypto_name,
        price_usd as current_price,
        market_cap,
        volume_24h,
        price_change_24h,
        price_change_pct_24h,
        market_cap_rank,
        extracted_at as last_updated,
        
        -- Calculate additional metrics
        CASE 
            WHEN price_change_pct_24h > 10 THEN 'Strong Gain'
            WHEN price_change_pct_24h > 5 THEN 'Moderate Gain'
            WHEN price_change_pct_24h > 0 THEN 'Slight Gain'
            WHEN price_change_pct_24h > -5 THEN 'Slight Loss'
            WHEN price_change_pct_24h > -10 THEN 'Moderate Loss'
            ELSE 'Strong Loss'
        END as price_trend_category,
        
        -- Market cap category
        CASE 
            WHEN market_cap_rank <= 10 THEN 'Large Cap'
            WHEN market_cap_rank <= 50 THEN 'Mid Cap'
            WHEN market_cap_rank <= 100 THEN 'Small Cap'
            ELSE 'Micro Cap'
        END as market_cap_category
        
    FROM latest_prices
    WHERE rn = 1
)

SELECT * FROM price_changes
  );
  
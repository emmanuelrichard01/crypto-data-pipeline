

SELECT 
    symbol,
    crypto_name,
    extraction_date,
    
    -- OHLC data (Open, High, Low, Close)
    FIRST_VALUE(price_usd) OVER (
        PARTITION BY symbol, extraction_date 
        ORDER BY extracted_at 
        ROWS UNBOUNDED PRECEDING
    ) as open_price,
    MAX(price_usd) as high_price,
    MIN(price_usd) as low_price,
    LAST_VALUE(price_usd) OVER (
        PARTITION BY symbol, extraction_date 
        ORDER BY extracted_at 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as close_price,
    
    -- Volume and market metrics
    AVG(volume_24h) as avg_volume_24h,
    AVG(market_cap) as avg_market_cap,
    AVG(market_cap_rank) as avg_market_cap_rank,
    
    -- Volatility measures
    STDDEV(price_usd) as daily_volatility,
    (MAX(price_usd) - MIN(price_usd)) / AVG(price_usd) * 100 as daily_range_pct,
    
    -- Data quality
    COUNT(*) as total_records,
    MIN(extracted_at) as first_extraction,
    MAX(extracted_at) as last_extraction

FROM "crypto_warehouse"."public_staging"."stg_crypto_prices"
GROUP BY symbol, crypto_name, extraction_date
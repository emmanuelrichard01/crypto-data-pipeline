create table "crypto_warehouse"."public_marts"."mart_crypto_latest_prices__dbt_tmp"


as

(


    with latest_prices as (
        select
            symbol,
            crypto_name,
            price_usd,
            market_cap,
            volume_24h,
            price_change_24h,
            price_change_pct_24h,
            market_cap_rank,
            extracted_at,

            ROW_NUMBER() over (
                partition by symbol
                order by extracted_at desc
            ) as rn

        from "crypto_warehouse"."public_staging"."stg_crypto_prices"
        where extraction_date = CURRENT_DATE
    ),

    price_changes as (
        select
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
            case
                when price_change_pct_24h > 10 then 'Strong Gain'
                when price_change_pct_24h > 5 then 'Moderate Gain'
                when price_change_pct_24h > 0 then 'Slight Gain'
                when price_change_pct_24h > -5 then 'Slight Loss'
                when price_change_pct_24h > -10 then 'Moderate Loss'
                else 'Strong Loss'
            end as price_trend_category,

            -- Market cap category
            case
                when market_cap_rank <= 10 then 'Large Cap'
                when market_cap_rank <= 50 then 'Mid Cap'
                when market_cap_rank <= 100 then 'Small Cap'
                else 'Micro Cap'
            end as market_cap_category

        from latest_prices
        where rn = 1
    )

    select * from price_changes
);

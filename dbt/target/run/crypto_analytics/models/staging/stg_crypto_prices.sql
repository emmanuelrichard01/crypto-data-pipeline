create table "crypto_warehouse"."public_staging"."stg_crypto_prices__dbt_tmp"


as

(


    with source_data as (
        select
            id as price_id,
            current_price as price_usd,
            market_cap,
            total_volume as volume_24h,
            price_change_24h,
            price_change_percentage_24h as price_change_pct_24h,
            market_cap_rank,
            extracted_at,
            created_at,
            UPPER(TRIM(symbol)) as symbol,
            TRIM(name) as crypto_name,

            COALESCE(current_price <= 0, false) as is_invalid_price,
            COALESCE(market_cap <= 0, false) as is_invalid_market_cap,
            COALESCE(total_volume < 0, false) as is_invalid_volume
        from "crypto_warehouse"."public"."crypto_prices_raw"
    ),

    cleaned_data as (
        select
            *,
            DATE(extracted_at) as extraction_date,
            DATE_TRUNC('hour', extracted_at) as extraction_hour,
            case
                when LAG(price_usd) over (
                    partition by symbol order by extracted_at
                ) is not null then
                    ((price_usd - LAG(price_usd) over (
                        partition by symbol order by extracted_at
                    )) / LAG(price_usd) over (
                        partition by symbol order by extracted_at
                    )) * 100
            end as price_change_pct_from_previous
        from source_data
        where not is_invalid_price and not is_invalid_volume
    )

    select * from cleaned_data
);

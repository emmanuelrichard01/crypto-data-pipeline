select
    count(*) as failures,
    count(*) != 0 as should_warn,
    count(*) != 0 as should_error
from (

    select *
    from
        "crypto_warehouse"."public_dbt_test__audit"."not_null_stg_crypto_prices_price_id"


) as dbt_internal_test

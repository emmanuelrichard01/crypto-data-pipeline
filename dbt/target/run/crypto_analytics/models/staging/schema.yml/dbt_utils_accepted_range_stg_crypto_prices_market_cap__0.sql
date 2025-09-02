select
    count(*) as failures,
    count(*) != 0 as should_warn,
    count(*) != 0 as should_error
from (

    select *
    from
        "crypto_warehouse"."public_dbt_test__audit"."dbt_utils_accepted_range_stg_crypto_prices_market_cap__0"


) as dbt_internal_test

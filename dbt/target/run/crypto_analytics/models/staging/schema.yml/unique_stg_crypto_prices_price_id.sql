select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "crypto_warehouse"."public_dbt_test__audit"."unique_stg_crypto_prices_price_id"
    
      
    ) dbt_internal_test
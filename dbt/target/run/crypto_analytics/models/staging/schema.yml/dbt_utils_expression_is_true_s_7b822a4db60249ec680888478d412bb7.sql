select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "crypto_warehouse"."public_dbt_test__audit"."dbt_utils_expression_is_true_s_7b822a4db60249ec680888478d412bb7"
    
      
    ) dbt_internal_test
select
    count(*) as failures,
    count(*) != 0 as should_warn,
    count(*) != 0 as should_error
from (

    select *
    from
        "crypto_warehouse"."public_dbt_test__audit"."not_null_stg_pipeline_runs_run_id"


) as dbt_internal_test

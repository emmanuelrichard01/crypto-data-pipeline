create view "crypto_warehouse"."public_staging"."stg_pipeline_runs__dbt_tmp"


as (
    select
        id as run_id,
        run_id as run_identifier,
        stage,
        status,
        records_processed,
        error_message,
        started_at,
        completed_at,
        case
            when completed_at is not NULL
                then EXTRACT(epoch from (completed_at - started_at))
        end as duration_seconds
    from "crypto_warehouse"."public"."pipeline_runs"
);

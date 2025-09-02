create table "crypto_warehouse"."public_marts"."mart_pipeline_monitoring__dbt_tmp"


as

(


    with pipeline_metrics as (
        select
            run_id,
            stage,
            status,
            records_processed,
            error_message,
            started_at,
            completed_at,
            EXTRACT(epoch from completed_at - started_at) as duration_seconds,

            -- Flag to compute success rate
            case when status = 'success' then 1 else 0 end as is_success,

            -- Classify performance
            case
                when completed_at is NULL then 'Running'
                when
                    EXTRACT(epoch from completed_at - started_at) < 30
                    then 'Fast'
                when
                    EXTRACT(epoch from completed_at - started_at) < 120
                    then 'Normal'
                else 'Slow'
            end as performance_category

        from "crypto_warehouse"."public_staging"."stg_pipeline_runs"
    ),

    latest_status_per_day_stage as (
        select
            stage,
            DATE(started_at) as run_date,
            FIRST_VALUE(status) over (
                partition by DATE(started_at), stage
                order by started_at desc
            ) as latest_run_status
        from pipeline_metrics
    ),

    daily_summary as (
        select
            stage,
            DATE(started_at) as run_date,

            COUNT(*) as total_runs,
            SUM(is_success) as successful_runs,
            SUM(records_processed) as total_records_processed,
            AVG(duration_seconds) as avg_duration_seconds,
            MAX(duration_seconds) as max_duration_seconds,
            ROUND(
                ((SUM(is_success)::FLOAT / COUNT(*)) * 100)::NUMERIC, 2
            ) as success_rate_pct


        from pipeline_metrics
        where started_at >= CURRENT_DATE - INTERVAL '30 days'
        group by DATE(started_at), stage
    )

    -- Final result combining metrics with latest run status
    select
        ds.*,
        ls.latest_run_status
    from daily_summary as ds
    left join latest_status_per_day_stage as ls
        on ds.run_date = ls.run_date and ds.stage = ls.stage
);

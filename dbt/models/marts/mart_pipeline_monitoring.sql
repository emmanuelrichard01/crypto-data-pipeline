{{ 
  config(
    materialized='table',
    indexes=[
      {'columns': ['run_date'], 'type': 'btree'},
      {'columns': ['stage'], 'type': 'btree'}
    ]
  ) 
}}

WITH pipeline_metrics AS (
    SELECT 
        run_id,
        stage,
        status,
        records_processed,
        error_message,
        started_at,
        completed_at,
        EXTRACT(EPOCH FROM completed_at - started_at) AS duration_seconds,

        -- Flag to compute success rate
        CASE WHEN status = 'success' THEN 1 ELSE 0 END AS is_success,

        -- Classify performance
        CASE 
            WHEN completed_at IS NULL THEN 'Running'
            WHEN EXTRACT(EPOCH FROM completed_at - started_at) < 30 THEN 'Fast'
            WHEN EXTRACT(EPOCH FROM completed_at - started_at) < 120 THEN 'Normal'
            ELSE 'Slow'
        END AS performance_category

    FROM {{ ref('stg_pipeline_runs') }}
),

latest_status_per_day_stage AS (
    SELECT
        DATE(started_at) AS run_date,
        stage,
        FIRST_VALUE(status) OVER (
            PARTITION BY DATE(started_at), stage
            ORDER BY started_at DESC
        ) AS latest_run_status
    FROM pipeline_metrics
),

daily_summary AS (
    SELECT
        DATE(started_at) AS run_date,
        stage,

        COUNT(*) AS total_runs,
        SUM(is_success) AS successful_runs,
        SUM(records_processed) AS total_records_processed,
        AVG(duration_seconds) AS avg_duration_seconds,
        MAX(duration_seconds) AS max_duration_seconds,
        ROUND(((SUM(is_success)::FLOAT / COUNT(*)) * 100)::NUMERIC, 2) AS success_rate_pct


    FROM pipeline_metrics
    WHERE started_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE(started_at), stage
)

-- Final result combining metrics with latest run status
SELECT
    ds.*,
    ls.latest_run_status
FROM daily_summary ds
LEFT JOIN latest_status_per_day_stage ls
  ON ds.run_date = ls.run_date AND ds.stage = ls.stage

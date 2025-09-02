SELECT
    id AS run_id,
    run_id AS run_identifier,
    stage,
    status,
    records_processed,
    error_message,
    started_at,
    completed_at,
    CASE
        WHEN completed_at IS NOT NULL
            THEN EXTRACT(EPOCH FROM (completed_at - started_at))
    END AS duration_seconds
FROM "crypto_warehouse"."public"."pipeline_runs"
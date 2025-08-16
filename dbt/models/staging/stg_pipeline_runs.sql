SELECT 
    id as run_id,
    run_id as run_identifier,
    stage,
    status,
    records_processed,
    error_message,
    started_at,
    completed_at,
    CASE 
        WHEN completed_at IS NOT NULL 
        THEN EXTRACT(EPOCH FROM (completed_at - started_at)) 
        ELSE NULL 
    END as duration_seconds
FROM {{ source('raw', 'pipeline_runs') }}
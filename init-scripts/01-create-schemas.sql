-- Create schemas for data organization
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS intermediate;

-- Create raw data source configuration for dbt
CREATE OR REPLACE VIEW information_schema.dbt_sources AS
SELECT
    'raw' AS source_name,
    'crypto_prices_raw' AS table_name,
    'public' AS schema_name
UNION ALL
SELECT
    'raw' AS source_name,
    'pipeline_runs' AS table_name,
    'public' AS schema_name;

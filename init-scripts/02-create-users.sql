-- Create additional users for different services
CREATE USER dbt_user WITH PASSWORD 'dbt_password_123';
CREATE USER dashboard_user WITH PASSWORD 'dashboard_password_123';

-- Grant permissions
GRANT CONNECT ON DATABASE crypto_warehouse TO dbt_user, dashboard_user;
GRANT USAGE ON SCHEMA public, staging, marts TO dbt_user, dashboard_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO dbt_user;
GRANT SELECT ON ALL TABLES IN SCHEMA staging, marts TO dashboard_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO dbt_user;

-- Set default privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT SELECT ON TABLES TO dashboard_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO dashboard_user;

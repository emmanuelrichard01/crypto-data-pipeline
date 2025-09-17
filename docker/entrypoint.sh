#!/bin/bash
set -e

# Wait for postgres with timeout
MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c '\q' 2>/dev/null; then
        echo "✅ PostgreSQL is ready"
        break
    fi
    echo "⏳ Waiting for PostgreSQL to be ready... ($((RETRY_COUNT + 1))/$MAX_RETRIES)"
    RETRY_COUNT=$((RETRY_COUNT + 1))
    sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "❌ Failed to connect to PostgreSQL after $MAX_RETRIES attempts"
    exit 1
fi

# Run database migrations
echo "🔄 Running database migrations..."
alembic upgrade head

# Database is ready, start the application
echo "✨ Starting application..."
exec "$@"

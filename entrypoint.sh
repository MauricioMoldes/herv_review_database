#!/bin/sh
set -e

echo "Waiting for Postgres to be ready..."
# Optional: simple retry loop
until pg_isready -h "$DB_HOST" -p 5432 -U "$DB_USER" > /dev/null 2>&1; do
    echo "Postgres is unavailable - sleeping 2 seconds..."
    sleep 2
done

echo "Running schema..."
psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -f /app/src/herv_database_schema.sql

echo "Loading HERV data..."
python /app/src/load_herv_data.py

echo "Done!"


#!/bin/bash
# ============================================================================
# Apache Airflow ETL Demo Platform - Airflow Entrypoint Script
# ============================================================================
# Purpose: Initialize Airflow environment and start services
# Usage: Called automatically by Docker container
# ============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "============================================"
echo "Airflow Entrypoint Script"
echo "============================================"

# Function to wait for PostgreSQL to be ready
wait_for_postgres() {
    local host=$1
    local port=$2
    local user=$3
    local db=$4
    local max_attempts=30
    local attempt=1

    echo -e "${YELLOW}Waiting for PostgreSQL at ${host}:${port}...${NC}"

    while [ $attempt -le $max_attempts ]; do
        if pg_isready -h "$host" -p "$port" -U "$user" -d "$db" > /dev/null 2>&1; then
            echo -e "${GREEN}PostgreSQL is ready!${NC}"
            return 0
        fi

        echo "Attempt $attempt/$max_attempts: PostgreSQL not ready yet..."
        sleep 2
        attempt=$((attempt + 1))
    done

    echo -e "${RED}ERROR: PostgreSQL failed to become ready after $max_attempts attempts${NC}"
    return 1
}

# Wait for Airflow metadata database
echo ""
echo "Step 1: Waiting for Airflow metadata database..."
wait_for_postgres "${POSTGRES_HOST:-airflow-postgres}" "${POSTGRES_PORT:-5432}" "airflow" "airflow"

# Wait for warehouse database
echo ""
echo "Step 2: Waiting for warehouse database..."
wait_for_postgres "${WAREHOUSE_HOST:-airflow-warehouse}" "${WAREHOUSE_PORT:-5432}" "${WAREHOUSE_USER:-warehouse_user}" "${WAREHOUSE_DB:-warehouse}"

# Initialize Airflow database (only for airflow-init service)
if [ "$_AIRFLOW_DB_MIGRATE" = "true" ]; then
    echo ""
    echo "Step 3: Initializing Airflow database..."
    airflow db migrate
    echo -e "${GREEN}Database migration complete!${NC}"
fi

# Create admin user (only for airflow-init service)
if [ "$_AIRFLOW_WWW_USER_CREATE" = "true" ]; then
    echo ""
    echo "Step 4: Creating Airflow admin user..."

    # Check if user already exists
    if airflow users list | grep -q "${_AIRFLOW_WWW_USER_USERNAME:-admin}"; then
        echo -e "${YELLOW}User '${_AIRFLOW_WWW_USER_USERNAME:-admin}' already exists, skipping...${NC}"
    else
        airflow users create \
            --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
            --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME:-Admin}" \
            --lastname "${_AIRFLOW_WWW_USER_LASTNAME:-User}" \
            --role Admin \
            --email "${_AIRFLOW_WWW_USER_EMAIL:-admin@example.com}" \
            --password "${_AIRFLOW_WWW_USER_PASSWORD:-admin}"
        echo -e "${GREEN}Admin user created!${NC}"
    fi
fi

# Create Airflow connections (if not already exist)
echo ""
echo "Step 5: Configuring Airflow connections..."

# Warehouse connection
if ! airflow connections get warehouse_default > /dev/null 2>&1; then
    echo "Creating warehouse connection..."
    airflow connections add warehouse_default \
        --conn-type postgres \
        --conn-host "${WAREHOUSE_HOST:-airflow-warehouse}" \
        --conn-port "${WAREHOUSE_PORT:-5432}" \
        --conn-login "${WAREHOUSE_USER:-warehouse_user}" \
        --conn-password "${WAREHOUSE_PASSWORD:-warehouse_pass}" \
        --conn-schema "${WAREHOUSE_DB:-warehouse}"
    echo -e "${GREEN}Warehouse connection created!${NC}"
else
    echo -e "${YELLOW}Warehouse connection already exists, skipping...${NC}"
fi

echo ""
echo "============================================"
echo -e "${GREEN}Airflow initialization complete!${NC}"
echo "============================================"
echo ""

# Execute the main command (passed as arguments to entrypoint)
exec "$@"
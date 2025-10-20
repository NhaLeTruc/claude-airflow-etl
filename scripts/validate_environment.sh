#!/bin/bash
# ============================================================================
# Environment Validation Script
# ============================================================================
# Purpose: Validate that Docker Compose environment is properly configured
# Usage: ./scripts/validate_environment.sh
# ============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "============================================"
echo "Apache Airflow ETL Demo - Environment Validation"
echo "============================================"
echo ""

# Check Docker is installed
echo -n "Checking Docker installation... "
if command -v docker &> /dev/null; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAILED${NC}"
    echo "Docker is not installed. Please install Docker first."
    exit 1
fi

# Check Docker Compose is installed
echo -n "Checking Docker Compose installation... "
if command -v docker compose &> /dev/null || command -v docker-compose &> /dev/null; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAILED${NC}"
    echo "Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Check if docker-compose.yml exists
echo -n "Checking docker-compose.yml exists... "
if [ -f "docker-compose.yml" ]; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAILED${NC}"
    echo "docker-compose.yml not found in current directory."
    exit 1
fi

# Validate docker-compose.yml syntax
echo -n "Validating docker-compose.yml syntax... "
if docker compose config > /dev/null 2>&1; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAILED${NC}"
    echo "docker-compose.yml has syntax errors. Run 'docker compose config' to see details."
    exit 1
fi

# Check required directories exist
echo ""
echo "Checking required directories..."
DIRS=("dags" "src" "tests" "logs" "plugins" "data" "docker")
for dir in "${DIRS[@]}"; do
    echo -n "  - $dir... "
    if [ -d "$dir" ]; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${YELLOW}MISSING (will be created)${NC}"
        mkdir -p "$dir"
    fi
done

# Check if services are running
echo ""
echo "Checking if Docker Compose services are running..."
SERVICES=("airflow-postgres" "airflow-warehouse" "airflow-webserver" "airflow-scheduler")

all_running=true
for service in "${SERVICES[@]}"; do
    echo -n "  - $service... "
    if docker ps --format '{{.Names}}' | grep -q "^${service}$"; then
        status=$(docker inspect --format='{{.State.Health.Status}}' "$service" 2>/dev/null || echo "no-healthcheck")
        if [ "$status" == "healthy" ] || [ "$status" == "no-healthcheck" ]; then
            echo -e "${GREEN}RUNNING${NC}"
        else
            echo -e "${YELLOW}RUNNING (health: $status)${NC}"
        fi
    else
        echo -e "${RED}NOT RUNNING${NC}"
        all_running=false
    fi
done

if [ "$all_running" = false ]; then
    echo ""
    echo -e "${YELLOW}Some services are not running. Start them with:${NC}"
    echo "  docker compose up -d"
    echo ""
fi

# Test database connectivity (if services are running)
if [ "$all_running" = true ]; then
    echo ""
    echo "Testing database connectivity..."

    echo -n "  - Airflow metadata database... "
    if docker exec airflow-postgres pg_isready -U airflow > /dev/null 2>&1; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FAILED${NC}"
    fi

    echo -n "  - Warehouse database... "
    if docker exec airflow-warehouse pg_isready -U warehouse_user -d warehouse > /dev/null 2>&1; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FAILED${NC}"
    fi

    # Test Airflow webserver
    echo -n "  - Airflow webserver (http://localhost:8080)... "
    if curl -s -f http://localhost:8080/health > /dev/null 2>&1; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${YELLOW}NOT READY${NC}"
    fi
fi

# Check for common issues
echo ""
echo "Checking for common issues..."

# Check if ports are available
echo -n "  - Port 8080 (Airflow UI)... "
if lsof -i:8080 > /dev/null 2>&1; then
    echo -e "${GREEN}IN USE${NC}"
else
    echo -e "${YELLOW}NOT IN USE${NC}"
fi

echo -n "  - Port 5433 (Warehouse)... "
if lsof -i:5433 > /dev/null 2>&1; then
    echo -e "${GREEN}IN USE${NC}"
else
    echo -e "${YELLOW}NOT IN USE${NC}"
fi

# Summary
echo ""
echo "============================================"
echo "Validation Complete"
echo "============================================"
echo ""

if [ "$all_running" = true ]; then
    echo -e "${GREEN}Environment is running and healthy!${NC}"
    echo ""
    echo "Access Airflow UI at: http://localhost:8080"
    echo "  Username: admin"
    echo "  Password: admin"
    echo ""
    echo "Connect to warehouse at: localhost:5433"
    echo "  Database: warehouse"
    echo "  User: warehouse_user"
    echo "  Password: warehouse_pass"
    echo ""
else
    echo -e "${YELLOW}Environment validation complete with warnings.${NC}"
    echo "Run 'docker compose up -d' to start services."
    echo ""
fi
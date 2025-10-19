# Database Migrations

This directory contains database migration scripts for the mock data warehouse schema versioning.

## Migration Naming Convention

Migrations are numbered sequentially:
- `001_initial_schema.sql` - Initial warehouse structure
- `002_add_indexes.sql` - Performance optimizations
- `003_add_columns.sql` - Schema changes
- etc.

## Running Migrations

Migrations are applied automatically when the PostgreSQL warehouse container initializes.

### Manual Migration

To run migrations manually:

```bash
# Connect to warehouse database
docker exec -it airflow-warehouse psql -U warehouse_user -d warehouse

# Run specific migration
\i /docker-entrypoint-initdb.d/migrations/001_initial_schema.sql

# Check migration status
SELECT * FROM warehouse.schema_migrations ORDER BY applied_at;
```

## Migration Workflow

1. Create new migration file with next sequential number
2. Add migration logic (DDL statements)
3. Record migration in `schema_migrations` table
4. Test migration on clean database
5. Commit migration file to version control

## Rollback

Migrations are forward-only. To rollback:
1. Create a new migration that reverses the changes
2. Or drop and recreate the database from schema.sql

## Best Practices

- Always test migrations on a copy of production data
- Include descriptive migration names
- Document breaking changes in migration comments
- Keep migrations idempotent when possible (use IF NOT EXISTS)
- Never modify existing migration files after they've been deployed

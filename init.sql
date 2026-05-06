-- SDA System database initialization
-- Runs once on first PostgreSQL container startup

-- Extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Tables are created by SQLAlchemy (init_db.py) on first API/worker startup,
-- AFTER this file runs.  Per-table autovacuum tuning for satellite_passes is
-- applied in init_db._apply_autovacuum_tuning(), which runs post-create_all().

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE sda_db TO sda_user;

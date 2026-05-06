"""
Database initializer — creates tables, indexes, seeds static data.

Safe to run multiple times (idempotent).  Intended as:
  - Docker entrypoint step before starting the API or workers
  - Local dev bootstrap: python -m sda_system.db.init_db
  - CI migration step

Steps (in order):
  1. Create all SQLAlchemy-mapped tables, indexes, and CHECK constraints.
  2. Verify expected tables are present and CHECK constraints are registered.
  3. Seed GroundStation rows from config.GROUND_STATIONS (skips existing).
  4. Seed a default dev APIKey when the table is empty (skipped in production).

This file does NOT run Alembic migrations.  Use `alembic upgrade head` for
schema changes on an existing database.
"""
import hashlib
import sys
from datetime import datetime, timezone
from typing import List, Set

from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError, OperationalError
from structlog import get_logger

from .models import Base, APIKey, GroundStation
from .session import engine, SessionLocal
from ..config import config

logger = get_logger()


# ---------------------------------------------------------------------------
# Step 1 — Table, index, and constraint creation
# ---------------------------------------------------------------------------

def create_tables() -> None:
    """
    Emit CREATE TABLE IF NOT EXISTS for every model in Base.metadata.

    SQLAlchemy also emits CREATE INDEX IF NOT EXISTS and the inline CHECK
    constraints declared in __table_args__.  Re-running on an existing DB
    is fully safe — existing tables are never dropped or truncated.
    """
    logger.info("Creating tables and indexes")
    try:
        Base.metadata.create_all(bind=engine, checkfirst=True)
    except OperationalError as exc:
        logger.error(
            "Table creation failed — check DATABASE_URL and Postgres connectivity",
            error=str(exc),
        )
        raise

    # Confirm every expected table exists after creation
    inspector = inspect(engine)
    existing: Set[str] = set(inspector.get_table_names())
    expected: Set[str] = {"tles", "ground_stations", "satellite_passes", "api_keys"}
    missing = expected - existing
    if missing:
        raise RuntimeError(
            f"CREATE TABLE appeared to succeed but these tables are absent: {missing}. "
            "Check that the database user has CREATE privileges."
        )

    logger.info("Tables present", tables=sorted(expected))


def _apply_autovacuum_tuning() -> None:
    """
    Apply per-table autovacuum tuning to satellite_passes.

    init.sql runs at Postgres first-boot, before SQLAlchemy creates tables, so
    the ALTER TABLE there is always a silent no-op.  Running here, after
    create_tables(), guarantees the table exists.  Non-fatal on failure so a
    permissions error does not abort the entire init sequence.
    """
    from sqlalchemy import text as sql_text

    try:
        with engine.connect() as conn:
            conn.execute(sql_text(
                "ALTER TABLE satellite_passes SET ("
                "  autovacuum_vacuum_scale_factor  = 0.01,"
                "  autovacuum_analyze_scale_factor = 0.005"
                ")"
            ))
            conn.commit()
        logger.info("Autovacuum tuning applied to satellite_passes")
    except OperationalError as exc:
        logger.warning(
            "Autovacuum tuning skipped — non-fatal (check ALTER privilege)",
            error=str(exc),
        )


# ---------------------------------------------------------------------------
# Step 2 — Constraint verification
# ---------------------------------------------------------------------------

def verify_constraints() -> None:
    """
    Confirm that all critical CHECK constraints are registered in the schema.

    Uses SQLAlchemy's inspect() to read the live catalog — no data is written.
    Raises RuntimeError if any constraint is missing so the operator knows
    immediately instead of discovering silent data corruption later.
    """
    inspector = inspect(engine)

    def names(table: str) -> Set[str]:
        return {c["name"] for c in inspector.get_check_constraints(table) if c["name"] is not None}

    required = {
        "ground_stations": {
            "ck_gs_latitude_range",
            "ck_gs_longitude_range",
            "ck_gs_altitude_non_negative",
        },
        "satellite_passes": {
            "ck_pass_time_order",
            "ck_pass_duration_positive",
            "ck_pass_elevation_range",
            "ck_pass_scheduled_by_values",
        },
        "api_keys": {
            "ck_apikey_hash_length",
            "ck_apikey_rate_positive",
        },
    }

    all_missing: Set[str] = set()
    for table, expected_names in required.items():
        live = names(table)
        missing = expected_names - live
        if missing:
            logger.error(
                "Missing CHECK constraints",
                table=table,
                missing=sorted(missing),
                live=sorted(live),
            )
            all_missing.update(missing)

    if all_missing:
        raise RuntimeError(
            f"Database is missing {len(all_missing)} CHECK constraint(s): "
            f"{sorted(all_missing)}. "
            "Drop and recreate the tables or run the Alembic migration."
        )

    logger.info("All CHECK constraints verified OK")


# ---------------------------------------------------------------------------
# Step 3 — Ground station seeding
# ---------------------------------------------------------------------------

def seed_ground_stations() -> None:
    """
    Insert GroundStation rows from config.GROUND_STATIONS.

    Only inserts stations whose station_id is not already in the DB.
    Uses bulk_save_objects for a single round-trip instead of 50 individual INSERTs.

    Raises on unexpected IntegrityError — most likely a station_id collision
    caused by a race between two init_db processes starting simultaneously.
    """
    configured = {s["id"]: s for s in config.GROUND_STATIONS}
    if not configured:
        logger.warning("config.GROUND_STATIONS is empty — nothing to seed")
        return

    db = SessionLocal()
    try:
        existing_ids: Set[str] = {
            row[0]
            for row in db.query(GroundStation.station_id).all()
        }

        to_insert: List[GroundStation] = []
        for station_id, s in configured.items():
            if station_id in existing_ids:
                continue
            to_insert.append(
                GroundStation(
                    station_id=s["id"],
                    name=s["name"],
                    latitude=float(s["latitude"]),
                    longitude=float(s["longitude"]),
                    altitude_m=float(s.get("altitude_m", 0.0)),
                    is_active=True,
                )
            )

        if not to_insert:
            logger.info(
                "Ground stations already seeded — nothing to insert",
                total_configured=len(configured),
            )
            return

        db.bulk_save_objects(to_insert)
        db.commit()
        logger.info(
            "Ground stations seeded",
            inserted=len(to_insert),
            skipped=len(existing_ids),
            total_configured=len(configured),
        )

    except IntegrityError as exc:
        db.rollback()
        logger.error(
            "Ground station seed hit IntegrityError — "
            "possible concurrent init_db or station_id collision",
            error=str(exc),
        )
        raise
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Step 4 — Default dev API key seeding
# ---------------------------------------------------------------------------

def seed_dev_api_key() -> None:
    """
    Insert a single default API key when the api_keys table is empty.

    Only runs outside production (ENVIRONMENT != 'production').  The raw key
    'dev-key-123' is SHA-256 hashed before storage, matching exactly what
    APIKeyAuth.verify() computes from the X-API-Key request header.

    Skipped silently if:
      - ENVIRONMENT == 'production'
      - The table already has at least one row (any key, not just this one)
    """
    if config.ENVIRONMENT.lower() == "production":
        logger.info("Production environment — skipping dev API key seed")
        return

    db = SessionLocal()
    try:
        existing_count = db.query(APIKey).count()
        if existing_count > 0:
            logger.info(
                "API keys already present — skipping dev seed",
                count=existing_count,
            )
            return

        raw_key = "dev-key-123"
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

        db.add(
            APIKey(
                key_hash=key_hash,
                name="dev-default",
                is_active=True,
                rate_limit_per_min=100,
                created_at=datetime.now(timezone.utc),
            )
        )
        db.commit()

        logger.warning(
            "Dev API key seeded — REMOVE before production",
            raw_key=raw_key,
            key_hash_prefix=key_hash[:8] + "...",
        )

    except IntegrityError as exc:
        db.rollback()
        # Race condition: another init_db inserted the same hash_key
        logger.warning(
            "Dev API key already inserted by concurrent process",
            error=str(exc),
        )
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def init_db() -> None:
    """
    Run all initialization steps in dependency order.

    Safe to call on every container startup — all steps are idempotent.
    Fails fast (raises) if any step fails so the container exits with a
    non-zero code instead of silently starting in a broken state.
    """
    logger.info(
        "Starting database initialization",
        environment=config.ENVIRONMENT,
    )

    create_tables()
    _apply_autovacuum_tuning()
    verify_constraints()
    seed_ground_stations()
    seed_dev_api_key()

    logger.info("Database initialization complete — all steps passed")


# ---------------------------------------------------------------------------
# CLI entry point:  python -m sda_system.db.init_db
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        init_db()
    except Exception as exc:
        logger.error("Database initialization FAILED", error=str(exc))
        sys.exit(1)

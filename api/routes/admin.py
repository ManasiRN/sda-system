"""
api/routes/admin.py — API key lifecycle management + direct pipeline trigger.

All endpoints require X-Admin-Key header matching ADMIN_API_KEY from config.
Raw keys are returned ONLY on creation; only the SHA-256 hash is stored.
"""

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query, Request, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

_admin_key_scheme = APIKeyHeader(name="X-Admin-Key", auto_error=False)

from ...config import settings
from ...db.models import APIKey
from ...db.session import get_db

router = APIRouter(prefix="/admin", tags=["admin"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class APIKeyResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: Optional[str]
    key_hash_prefix: str
    is_active: bool
    rate_limit_per_min: int
    created_at: datetime
    last_used_at: Optional[datetime] = None


class APIKeyCreateRequest(BaseModel):
    name: str
    rate_limit_per_min: int = 100


class APIKeyCreateResponse(APIKeyResponse):
    raw_key: str


class APIKeyUpdateRequest(BaseModel):
    rate_limit_per_min: Optional[int] = None
    is_active: Optional[bool] = None


# ---------------------------------------------------------------------------
# Admin authentication dependency
# ---------------------------------------------------------------------------

def _require_admin(x_admin_key: Optional[str] = Depends(_admin_key_scheme)) -> None:
    """
    Verify X-Admin-Key header against ADMIN_API_KEY config value.

    Uses secrets.compare_digest to prevent timing-oracle attacks.
    """
    if not x_admin_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Admin-Key header",
        )
    expected = settings.ADMIN_API_KEY.get_secret_value()
    if not secrets.compare_digest(x_admin_key, expected):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid admin key",
        )


def _to_response(key: APIKey) -> APIKeyResponse:
    return APIKeyResponse(
        id=key.id,
        name=key.name,
        key_hash_prefix=key.key_hash[:8] + "...",
        is_active=key.is_active,
        rate_limit_per_min=key.rate_limit_per_min,
        created_at=key.created_at,
        last_used_at=key.last_used_at,
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/api-keys", response_model=List[APIKeyResponse])
def list_api_keys(
    db: Session = Depends(get_db),
    _: None = Depends(_require_admin),
) -> List[APIKeyResponse]:
    """List all API keys. Raw key values are never returned."""
    keys = db.query(APIKey).order_by(APIKey.created_at.desc()).all()
    return [_to_response(k) for k in keys]


@router.post("/api-keys", response_model=APIKeyCreateResponse, status_code=status.HTTP_201_CREATED)
def create_api_key(
    payload: APIKeyCreateRequest,
    db: Session = Depends(get_db),
    _: None = Depends(_require_admin),
) -> APIKeyCreateResponse:
    """
    Create a new API key.

    The raw key is returned exactly once in this response.
    It is NOT stored — only the SHA-256 hash is persisted.
    Record the raw key immediately; it cannot be recovered later.
    """
    raw_key  = secrets.token_urlsafe(32)
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

    key = APIKey(
        key_hash=key_hash,
        name=payload.name,
        is_active=True,
        rate_limit_per_min=payload.rate_limit_per_min,
        created_at=datetime.now(timezone.utc),
    )
    db.add(key)
    db.commit()
    db.refresh(key)

    return APIKeyCreateResponse(
        id=key.id,
        name=key.name,
        key_hash_prefix=key.key_hash[:8] + "...",
        is_active=key.is_active,
        rate_limit_per_min=key.rate_limit_per_min,
        created_at=key.created_at,
        last_used_at=key.last_used_at,
        raw_key=raw_key,
    )


@router.patch("/api-keys/{key_id}", response_model=APIKeyResponse)
def update_api_key(
    key_id: int,
    payload: APIKeyUpdateRequest,
    db: Session = Depends(get_db),
    _: None = Depends(_require_admin),
) -> APIKeyResponse:
    """Update rate limit or active status of an existing key."""
    key = db.query(APIKey).filter(APIKey.id == key_id).first()
    if not key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"API key {key_id} not found",
        )

    if payload.rate_limit_per_min is not None:
        key.rate_limit_per_min = payload.rate_limit_per_min
    if payload.is_active is not None:
        key.is_active = payload.is_active

    db.commit()
    db.refresh(key)
    return _to_response(key)


@router.post("/tasks/schedule", status_code=status.HTTP_200_OK)
def trigger_schedule_only(
    _: None = Depends(_require_admin),
) -> Dict[str, Any]:
    """
    Run greedy scheduler synchronously over a 7-day window.
    Returns diagnostic counts immediately — use this to fix sched=0.
    """
    from sda_system.db.session import SessionLocal
    from sda_system.scheduling.greedy import GreedyScheduler

    db = SessionLocal()
    try:
        now      = datetime.now(timezone.utc)
        end_time = now + timedelta(days=7)
        db.expire_all()
        scheduled, sat_ids = GreedyScheduler(session=db).schedule_all_stations(now, end_time)
        return {
            "status":            "ok",
            "window_start":      now.isoformat(),
            "window_end":        end_time.isoformat(),
            "passes_scheduled":  len(scheduled),
            "unique_satellites": len(sat_ids),
        }
    except Exception as exc:
        return {"status": "error", "detail": str(exc), "type": type(exc).__name__}
    finally:
        db.close()


@router.post("/tasks/ingest-tles", status_code=status.HTTP_202_ACCEPTED)
async def trigger_ingest_tles(
    background_tasks: BackgroundTasks,
    _: None = Depends(_require_admin),
) -> Dict[str, Any]:
    """
    Trigger TLE ingestion directly in the API process (no Celery worker needed).
    Returns immediately; ingestion runs in the background (~20-40s).
    Check /health afterwards to confirm TLE data is present.
    """
    from sda_system.ingestion.fetcher import TLEFetcher
    from sda_system.db.session import SessionLocal

    async def _run_ingest() -> None:
        db = SessionLocal()
        try:
            fetcher = TLEFetcher(db)
            fetcher.init_ground_stations()
            await fetcher.fetch_all()
        finally:
            db.close()

    background_tasks.add_task(_run_ingest)
    return {"status": "accepted", "message": "TLE ingestion started — check /health in ~60s"}


@router.post("/tasks/run-pipeline", status_code=status.HTTP_202_ACCEPTED)
async def trigger_run_pipeline(
    background_tasks: BackgroundTasks,
    limit: int = Query(200, ge=1, le=20000, description="Max satellites to process"),
    _: None = Depends(_require_admin),
) -> Dict[str, Any]:
    """
    Run the full pipeline directly: detect passes + greedy schedule.
    No Celery workers required. Takes 2-10 min depending on limit.
    Check /api/coverage afterwards to see results.
    """
    from sda_system.db.models import TLE, GroundStation, SatellitePass
    from sda_system.db.session import SessionLocal
    from sda_system.propagation.pass_detector import pass_detector
    from sda_system.propagation.sgp4_engine import sgp4_engine
    from sda_system.scheduling.greedy import GreedyScheduler
    from sda_system.config import config as sda_config

    def _run() -> None:
        import structlog as _structlog
        from sqlalchemy import func
        _log = _structlog.get_logger()
        _log.info("run_pipeline_started", limit=limit)
        db = SessionLocal()
        try:
            now      = datetime.now(timezone.utc)
            end_time = now + timedelta(days=sda_config.PROPAGATION_DAYS)

            tles     = db.query(TLE).filter(TLE.is_current == True).order_by(func.random()).limit(limit).all()  # noqa: E712
            stations = db.query(GroundStation).filter(GroundStation.is_active == True).all()  # noqa: E712

            if not tles or not stations:
                return

            from sqlalchemy.dialects.postgresql import insert as pg_insert
            from sqlalchemy import Table
            from typing import cast as tcast

            _table: Table = tcast(Table, SatellitePass.__table__)

            for tle in tles:
                try:
                    result = sgp4_engine.propagate(
                        line1=tle.line1, line2=tle.line2,
                        norad_id=tle.norad_id, start_time=now, name=tle.name or "",
                    )
                except Exception:
                    continue

                if not result.is_usable() or len(result.valid_positions) == 0:
                    continue

                rows: List[Dict] = []
                for station in stations:
                    station_dict = {
                        "id":                 station.station_id,
                        "latitude":           station.latitude,
                        "longitude":          station.longitude,
                        "altitude_m":         station.altitude_m,
                        "elevation_mask_deg": getattr(station, "elevation_mask_deg", sda_config.MIN_ELEVATION_DEG),
                    }
                    try:
                        detected = pass_detector.detect_passes(
                            result.valid_positions, result.valid_times, station_dict
                        )
                    except Exception:
                        continue
                    for p in detected:
                        rows.append({
                            "norad_id":           tle.norad_id,
                            "station_id":         station.station_id,
                            "rise_time":          p["rise_time"],
                            "set_time":           p["set_time"],
                            "duration_seconds":   p["duration_seconds"],
                            "max_elevation":      p["max_elevation"],
                            "max_elevation_time": p["max_elevation_time"],
                            "azimuth_at_rise":    p.get("azimuth_at_rise"),
                            "azimuth_at_set":     p.get("azimuth_at_set"),
                            "azimuth_at_max":     p.get("azimuth_at_max"),
                            "is_scheduled":       False,
                            "tle_epoch":          tle.epoch,
                        })

                if rows:
                    stmt = pg_insert(_table).values(rows).on_conflict_do_nothing(
                        index_elements=["norad_id", "station_id", "rise_time"]
                    )
                    db.execute(stmt)
                    db.commit()

            # Greedy scheduling pass
            scheduler = GreedyScheduler(session=db)
            scheduler.schedule_all_stations(now, end_time)
            _log.info("run_pipeline_completed", limit=limit)

        except Exception as exc:
            _log.error("run_pipeline_failed", error=str(exc), error_type=type(exc).__name__)
        finally:
            db.close()

    background_tasks.add_task(_run)
    return {
        "status":  "accepted",
        "message": f"Pipeline started for up to {limit} satellites — check /api/coverage in ~5 min",
    }


@router.post("/tasks/debug-pipeline")
def debug_pipeline(_: None = Depends(_require_admin)) -> Dict[str, Any]:
    """Run pipeline for 1 satellite synchronously and return diagnostic result."""
    try:
        from sda_system.db.models import TLE, GroundStation, SatellitePass
        from sda_system.db.session import SessionLocal
        from sda_system.propagation.pass_detector import pass_detector
        from sda_system.propagation.sgp4_engine import sgp4_engine
        from sda_system.config import config as sda_config
    except Exception as exc:
        return {"error": "import_failed", "detail": str(exc)}

    db = SessionLocal()
    try:
        now  = datetime.now(timezone.utc)
        tle  = db.query(TLE).filter(TLE.is_current == True).first()  # noqa: E712
        if not tle:
            return {"error": "no_tles_in_db"}
        stations = db.query(GroundStation).filter(GroundStation.is_active == True).limit(3).all()  # noqa: E712
        if not stations:
            return {"error": "no_stations_in_db"}
        try:
            result = sgp4_engine.propagate(
                line1=tle.line1, line2=tle.line2,
                norad_id=tle.norad_id, start_time=now, name=tle.name or "",
            )
        except Exception as exc:
            return {"error": "propagation_failed", "detail": str(exc)}
        if not result.is_usable():
            return {"error": "propagation_not_usable", "valid_pct": result.valid_fraction}
        passes_found = 0
        for station in stations:
            station_dict = {
                "id": station.station_id, "latitude": station.latitude,
                "longitude": station.longitude, "altitude_m": station.altitude_m,
                "elevation_mask_deg": getattr(station, "elevation_mask_deg", sda_config.MIN_ELEVATION_DEG),
            }
            detected = pass_detector.detect_passes(result.valid_positions, result.valid_times, station_dict)
            passes_found += len(detected)
        return {
            "status": "ok",
            "tle_norad_id": tle.norad_id,
            "valid_positions": len(result.valid_positions),
            "stations_checked": len(stations),
            "passes_found": passes_found,
        }
    except Exception as exc:
        return {"error": str(exc), "type": type(exc).__name__}
    finally:
        db.close()


@router.post("/tasks/cleanup-passes", status_code=status.HTTP_200_OK)
def cleanup_passes(
    keep_days: int = Query(7, ge=1, le=30, description="Keep passes within this many days from now"),
    _: None = Depends(_require_admin),
) -> Dict[str, Any]:
    """Delete satellite passes outside the active scheduling window to free disk space."""
    from sda_system.db.models import SatellitePass
    from sda_system.db.session import SessionLocal
    from sqlalchemy import text

    db = SessionLocal()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=1)
        result = db.execute(
            text("DELETE FROM satellite_passes WHERE rise_time < :cutoff"),
            {"cutoff": cutoff}
        )
        db.commit()
        deleted = result.rowcount
        remaining = db.query(SatellitePass).count()
        db.execute(text("VACUUM satellite_passes"))
        db.commit()
        return {"status": "ok", "deleted": deleted, "remaining": remaining}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}
    finally:
        db.close()


@router.delete("/api-keys/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
def revoke_api_key(
    key_id: int,
    db: Session = Depends(get_db),
    _: None = Depends(_require_admin),
) -> None:
    """
    Revoke (deactivate) an API key.

    Sets is_active=False. The key is retained in the DB for audit purposes.
    Reactivate via PATCH /admin/api-keys/{key_id} if revoked in error.
    """
    key = db.query(APIKey).filter(APIKey.id == key_id).first()
    if not key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"API key {key_id} not found",
        )

    key.is_active = False
    db.commit()

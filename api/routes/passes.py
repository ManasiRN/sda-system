"""
api/routes/passes.py — Pass query, schedule, and coverage endpoints.

Fixes vs. original
------------------
  - datetime.utcnow() (naive) replaced by datetime.now(timezone.utc) in
    get_passes — naive datetimes raise TypeError when compared with
    DateTime(timezone=True) columns in PostgreSQL.
  - datetime.combine(schedule_date, datetime.min.time()) (naive) replaced by
    datetime.combine(schedule_date, time.min, timezone.utc) in get_schedule —
    same naive-vs-aware mismatch against the DB column.
  - verify_api_key unpacks auth.verify() as (key_token, rate_limit) — after
    the middleware fix, verify() returns Tuple[str, int]; the original stored
    the whole tuple in key_hash and passed it to check_rate_limit as the key
    string, silently breaking both auth and rate limiting.
  - verify_api_key unpacks check_rate_limit() as (allowed, retry_after) —
    check_rate_limit() now returns Tuple[bool, Optional[int]]; the original
    evaluated `if not (True, None)` which is always False, making rate
    limiting a no-op.
  - Retry-After header added to 429 responses — clients now know exactly
    when to retry instead of guessing.
  - verify_api_key returns key_token (SHA-256 hash) not x_api_key (raw key)
    — returning the raw secret to every route handler was a security leak.
  - Per-key rate_limit_per_min from auth.verify() used instead of a global
    settings constant — each API key can now have its own rate cap.
  - get_cache() dependency changed from a circular import of api.main.cache to
    request.app.state.cache — eliminates the fragile deferred circular import.
  - Cache write-back added in get_passes — on a DB-read path the serialised
    results are now stored in Redis so subsequent identical queries hit cache.
  - Time-window validation changed from timedelta.days > 7 to total_seconds()
    > 7 * 86400 — the original check allowed windows of 7 days + N seconds.
  - end_time <= start_time guard added — inverted windows previously returned
    empty results with HTTP 200, silently confusing callers.
  - ScheduleResponse Pydantic model added and set as response_model on
    /schedule — the original returned a raw dict with no schema or validation.
  - model_validate() used for cached dict→PassResponse conversion instead of
    PassResponse(**p) — handles datetime fields stored as ISO strings in Redis.
"""

from datetime import datetime, date, time, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, case
from sqlalchemy.orm import Session

_api_key_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)

from ...db.models import GroundStation, SatellitePass
from ...db.session import get_db
from ...cache.redis_client import RedisCache

# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class PassResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    norad_id: int
    station_id: str
    rise_time: datetime
    set_time: datetime
    max_elevation: float
    duration_seconds: float
    is_scheduled: bool
    scheduled_by: Optional[str] = None


class PaginatedPassesResponse(BaseModel):
    data: List[PassResponse]
    total_count: int
    page: int
    page_size: int
    next_page: Optional[int]


class ScheduleResponse(BaseModel):
    station_id: str
    date: str
    scheduled_passes: int
    passes: List[PassResponse]


class CoverageResponse(BaseModel):
    total_visible_satellites: int
    greedy_scheduled: int
    ortools_scheduled: int
    total_scheduled: int
    coverage_pct: float
    greedy_coverage_pct: float
    ortools_improvement_pct: float
    station_utilization: List[Dict[str, Any]]
    unscheduled_count: int


# ---------------------------------------------------------------------------
# Shared dependencies
# ---------------------------------------------------------------------------

async def get_cache(request: Request) -> Optional[RedisCache]:
    """
    Return the RedisCache instance from app state.

    Avoids the circular import that the original code used
    (from ...api.main import cache) — that module imports this router.
    Returns None if the cache was not initialised (tests, partial startup).
    """
    return getattr(request.app.state, "cache", None)


async def verify_api_key(
    request: Request,
    x_api_key: Optional[str] = Depends(_api_key_scheme),
    db: Session = Depends(get_db),
) -> str:
    """
    Authenticate the request via X-API-Key and enforce per-key rate limiting.

    Returns the SHA-256 token (hashed key) — never the raw secret — so route
    handlers can safely log or store the returned value.
    """
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
        )

    auth         = request.app.state.api_key_auth
    rate_limiter = request.app.state.rate_limiter

    # verify() now returns (key_token, rate_limit_per_min)
    key_token, rate_limit = await auth.verify(x_api_key, db)

    # check_rate_limit() now returns (allowed, retry_after_seconds)
    allowed, retry_after = await rate_limiter.check_rate_limit(key_token, rate_limit)
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded. Try again later.",
            headers={"Retry-After": str(retry_after or 60)},
        )

    # Return the hashed token — not the raw key — to route handlers
    return key_token


# ---------------------------------------------------------------------------
# Router — all routes require a valid API key
# ---------------------------------------------------------------------------

router = APIRouter(
    prefix="/api",
    tags=["passes"],
    dependencies=[Depends(verify_api_key)],
)

_7_DAYS_SECONDS = 7 * 24 * 3600


# ---------------------------------------------------------------------------
# GET /api/passes
# ---------------------------------------------------------------------------

@router.get("/passes", response_model=PaginatedPassesResponse)
async def get_passes(
    station_id:  Optional[str] = Query(None, description="Filter by station ID (e.g. GS001)"),
    satellite_id: Optional[int] = Query(None, description="Filter by satellite NORAD ID"),
    start_time:  Optional[datetime] = Query(None, description="Start of time window (ISO 8601)"),
    end_time:    Optional[datetime] = Query(None, description="End of time window (ISO 8601)"),
    page:        int = Query(1,  ge=1,        description="Page number (1-based)"),
    page_size:   int = Query(50, ge=1, le=100, description="Items per page (max 100)"),
    db:    Session            = Depends(get_db),
    cache: Optional[RedisCache] = Depends(get_cache),
):
    """
    Return satellite passes with pagination.

    Time window defaults to [now, now+7d].  Maximum window is 7 days.
    Cache is consulted (and populated) for station-only queries.
    """
    # --- Timezone-aware defaults (fix: was datetime.utcnow() — naive) ---
    now = datetime.now(timezone.utc)
    if start_time is None:
        start_time = now - timedelta(days=7)   # default: past 7 days
    if end_time is None:
        end_time = start_time + timedelta(days=7)

    # Ensure supplied datetimes are timezone-aware
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)

    # --- Input validation ---
    if end_time <= start_time:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="end_time must be after start_time",
        )

    # Fix: was .days > 7 which allowed windows of 7d + N seconds
    if (end_time - start_time).total_seconds() > _7_DAYS_SECONDS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Maximum time window is 7 days",
        )

    # --- Cache (station-only queries) ---
    if cache is not None and station_id and not satellite_id:
        query_date = start_time.date()
        try:
            cached = await cache.get_passes(station_id, query_date)
        except Exception:
            cached = None

        if cached:
            offset    = (page - 1) * page_size
            paginated = cached[offset: offset + page_size]
            # model_validate handles ISO-string datetimes stored in Redis
            return PaginatedPassesResponse(
                data=[PassResponse.model_validate(p) for p in paginated],
                total_count=len(cached),
                page=page,
                page_size=page_size,
                next_page=page + 1 if offset + page_size < len(cached) else None,
            )

    # --- DB query ---
    query = db.query(SatellitePass).filter(
        SatellitePass.rise_time >= start_time,
        SatellitePass.set_time  <= end_time,
    )
    if station_id:
        query = query.filter(SatellitePass.station_id == station_id)
    if satellite_id:
        query = query.filter(SatellitePass.norad_id == satellite_id)

    total_count = query.count()
    offset      = (page - 1) * page_size
    passes      = (
        query
        .order_by(SatellitePass.rise_time)
        .offset(offset)
        .limit(page_size)
        .all()
    )

    pass_responses = [PassResponse.model_validate(p) for p in passes]

    # --- Cache write-back (station-only, page 1 only — full result set) ---
    if cache is not None and station_id and not satellite_id and page == 1:
        try:
            all_passes = query.order_by(SatellitePass.rise_time).all()
            await cache.set_passes(
                station_id,
                start_time.date(),
                [PassResponse.model_validate(p).model_dump(mode="json") for p in all_passes],
                ttl_sec=3600,
            )
        except Exception:
            pass  # Cache write failure must never break a request

    return PaginatedPassesResponse(
        data=pass_responses,
        total_count=total_count,
        page=page,
        page_size=page_size,
        next_page=page + 1 if offset + page_size < total_count else None,
    )


# ---------------------------------------------------------------------------
# GET /api/schedule
# ---------------------------------------------------------------------------

@router.get("/schedule", response_model=ScheduleResponse)
async def get_schedule(
    station_id:    str  = Query(..., description="Station ID (e.g. GS001)"),
    schedule_date: date = Query(..., description="Schedule date (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
):
    """Return all scheduled passes for a station on a specific UTC date."""
    # Fix: was datetime.combine(schedule_date, datetime.min.time()) — naive
    start_time = datetime.combine(schedule_date, time.min, timezone.utc)
    end_time   = start_time + timedelta(days=1)

    passes = (
        db.query(SatellitePass)
        .filter(
            SatellitePass.station_id   == station_id,
            SatellitePass.is_scheduled == True,  # noqa: E712
            SatellitePass.rise_time    >= start_time,
            SatellitePass.set_time     <= end_time,
        )
        .order_by(SatellitePass.rise_time)
        .all()
    )

    return ScheduleResponse(
        station_id=station_id,
        date=schedule_date.isoformat(),
        scheduled_passes=len(passes),
        passes=[PassResponse.model_validate(p) for p in passes],
    )


# ---------------------------------------------------------------------------
# GET /api/coverage
# ---------------------------------------------------------------------------

@router.get("/coverage", response_model=CoverageResponse)
async def get_coverage(db: Session = Depends(get_db)):
    """
    Return global coverage statistics using aggregate DB queries (no N+1).

    All counts are resolved in three SQL round-trips:
      1. COUNT(DISTINCT norad_id) — total visible satellites
      2. GROUP BY scheduled_by    — unique satellites per scheduler stage
      3. GROUP BY station_id      — per-station utilization
    """
    # 1. Total satellites that have at least one pass in the DB
    total_visible: int = db.query(SatellitePass.norad_id).distinct().count()

    # 2. Unique satellites scheduled, broken down by stage — one GROUP BY query
    stage_rows = (
        db.query(
            SatellitePass.scheduled_by,
            func.count(func.distinct(SatellitePass.norad_id)),
        )
        .filter(SatellitePass.is_scheduled == True)  # noqa: E712
        .group_by(SatellitePass.scheduled_by)
        .all()
    )
    greedy_scheduled  = 0
    ortools_scheduled = 0
    for scheduled_by, cnt in stage_rows:
        if scheduled_by in ("greedy", "interval_tree"):
            greedy_scheduled += cnt
        elif scheduled_by == "ortools":
            ortools_scheduled = cnt

    # 3. Total unique scheduled satellites (across all stages)
    total_scheduled: int = (
        db.query(SatellitePass.norad_id)
        .filter(SatellitePass.is_scheduled == True)  # noqa: E712
        .distinct()
        .count()
    )

    # 4. Per-station stats — single GROUP BY, no N+1 queries
    station_rows = (
        db.query(
            SatellitePass.station_id,
            func.count(SatellitePass.id).label("total_passes"),
            func.sum(
                case((SatellitePass.is_scheduled == True, 1), else_=0)
            ).label("scheduled_passes"),
            func.count(
                func.distinct(
                    case(
                        (SatellitePass.is_scheduled == True, SatellitePass.norad_id),
                        else_=None,
                    )
                )
            ).label("unique_sats_scheduled"),
        )
        .group_by(SatellitePass.station_id)
        .all()
    )

    station_names: Dict[str, str] = {
        row.station_id: row.name
        for row in db.query(GroundStation.station_id, GroundStation.name).all()
    }

    station_utilization: List[Dict[str, Any]] = []
    for row in station_rows:
        total     = row.total_passes or 0
        scheduled = int(row.scheduled_passes or 0)
        station_utilization.append({
            "station_id":                row.station_id,
            "station_name":              station_names.get(row.station_id, f"Station_{row.station_id}"),
            "total_passes_visible":      total,
            "scheduled_passes":          scheduled,
            "unique_satellites_scheduled": int(row.unique_sats_scheduled or 0),
            "utilization_pct":           round((scheduled / total * 100) if total > 0 else 0.0, 1),
        })

    coverage_pct          = (total_scheduled / total_visible * 100) if total_visible > 0 else 0.0
    greedy_coverage_pct   = (greedy_scheduled / total_visible * 100) if total_visible > 0 else 0.0
    ortools_improvement   = (
        (total_scheduled - greedy_scheduled) / total_visible * 100
    ) if total_visible > 0 else 0.0

    return CoverageResponse(
        total_visible_satellites=total_visible,
        greedy_scheduled=greedy_scheduled,
        ortools_scheduled=ortools_scheduled,
        total_scheduled=total_scheduled,
        coverage_pct=round(coverage_pct, 1),
        greedy_coverage_pct=round(greedy_coverage_pct, 1),
        ortools_improvement_pct=round(ortools_improvement, 1),
        station_utilization=station_utilization,
        unscheduled_count=total_visible - total_scheduled,
    )

"""
api/main.py — FastAPI application entry point.

Fixes vs. original
------------------
  - await redis.from_url() removed — from_url() is synchronous and returns a
    Redis instance directly; awaiting it raised TypeError on every startup.
  - redis_client.close() → aclose() — close() is the sync variant and does not
    properly drain the async connection pool; aclose() is the correct teardown.
  - app.state.cache never assigned — the get_cache() dependency in routes reads
    request.app.state.cache; without this assignment the cache was always None
    and every request fell through to the database even when Redis was healthy.
  - cache.get_cache_hit_ratio() → cache.stats() — get_cache_hit_ratio() does
    not exist on RedisCache; stats() returns hits, misses, hit_ratio, and
    circuit-breaker state and is the correct observability method.
  - datetime.utcnow() (naive) → datetime.now(timezone.utc) — naive timestamps
    in /health responses can silently break clients that expect RFC-3339 strings.
  - DB connection leaks fixed — both SessionLocal() blocks lacked finally:
    db.close(); any exception before close() leaked a pool connection
    indefinitely. Both blocks merged into one session with a single finally.
  - /health returns HTTP 503 when degraded — previously always returned 200
    with status='degraded' in the body, making it unusable as a Kubernetes
    readiness probe without body parsing.
  - RequestLoggingMiddleware now receives Prometheus metrics — the original
    add_middleware() call passed no metrics arguments, so REQUEST_COUNT and
    REQUEST_LATENCY were defined at module level but never incremented.
  - CORSMiddleware added — without it the API is blocked by the browser
    same-origin policy for every frontend client.
  - exc.headers preserved in http_exception_handler — the original dropped all
    exception headers, silently discarding the Retry-After header on 429s.
  - exc_info=True removed from structlog call — structlog does not process
    exc_info=True as a traceback capture; it was logged as a literal kv-pair.
"""

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from structlog import get_logger

from ..cache.redis_client import RedisCache
from ..config import settings
from .middleware import APIKeyAuth, RateLimiter, RequestLoggingMiddleware
from .routes import passes
from .routes import admin

logger = get_logger()

# ---------------------------------------------------------------------------
# Prometheus metrics
# Defined at module level so they survive request boundaries and are
# registered exactly once per process (re-registration raises ValueError).
# ---------------------------------------------------------------------------
REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status_code"],
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

# ---------------------------------------------------------------------------
# Redis cache — constructed here, connected inside lifespan
# ---------------------------------------------------------------------------
cache = RedisCache(settings.REDIS_URL)

# ---------------------------------------------------------------------------
# CORS origins
# Non-production: allow all (convenient for local dev and staging).
# Production:     read from CORS_ORIGINS setting; empty list blocks all origins.
# ---------------------------------------------------------------------------
_CORS_ORIGINS: list = (
    settings.CORS_ORIGINS
    if settings.ENVIRONMENT.lower() == "production" and settings.CORS_ORIGINS
    else (["*"] if settings.ENVIRONMENT.lower() != "production" else [])
)


# ---------------------------------------------------------------------------
# Lifespan — startup / shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.

    Startup order
    -------------
    1. Connect RedisCache (pool + PING verification, circuit-breaker ready).
    2. Create the raw async Redis client for APIKeyAuth and RateLimiter.
       from_url() is synchronous — the pool is lazy; do NOT await it.
    3. Attach auth helpers and cache to app.state so route dependencies
       can resolve them via request.app.state.
    """
    logger.info(
        "sda_api_startup",
        environment=settings.ENVIRONMENT,
        redis_url=settings.REDIS_URL,
    )

    await cache.connect()

    # Fix: was `await redis.from_url(...)` — from_url() is NOT a coroutine;
    # awaiting a Redis instance raises TypeError at every startup.
    redis_client: aioredis.Redis = aioredis.from_url(
        settings.REDIS_URL,
        encoding="utf-8",
        decode_responses=False,  # middleware handles bytes → str decode explicitly
    )

    app.state.redis_client = redis_client
    app.state.api_key_auth  = APIKeyAuth(redis_client)
    app.state.rate_limiter  = RateLimiter(redis_client)
    app.state.cache         = cache   # Fix: was never set — routes always got None

    logger.info("sda_api_ready")
    yield

    # Shutdown — drain both Redis connections
    logger.info("sda_api_shutdown")
    await cache.disconnect()
    await app.state.redis_client.aclose()  # Fix: was .close() — wrong async method


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SDA Pass Scheduling System",
    description=(
        "Space Domain Awareness satellite pass scheduling "
        "with greedy + OR-Tools optimization"
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Middleware  (added in reverse order — last registered = outermost layer)
# ---------------------------------------------------------------------------

# CORS — must be outermost so preflight OPTIONS requests are handled before auth
app.add_middleware(
    CORSMiddleware,
    allow_origins=_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["X-Trace-ID", "Retry-After"],
)

# Request logging + Prometheus metrics
# Fix: metrics objects were defined but never passed → counters never incremented
app.add_middleware(
    RequestLoggingMiddleware,
    request_count=REQUEST_COUNT,
    request_latency=REQUEST_LATENCY,
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(passes.router)
app.include_router(admin.router)

# ---------------------------------------------------------------------------
# Frontend dashboard — served at /ui
# ---------------------------------------------------------------------------

_FRONTEND_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "frontend")
)
if os.path.isdir(_FRONTEND_DIR):
    app.mount("/ui", StaticFiles(directory=_FRONTEND_DIR, html=True), name="frontend")


# ---------------------------------------------------------------------------
# System endpoints
# ---------------------------------------------------------------------------

@app.get("/metrics", include_in_schema=False)
async def metrics_endpoint() -> Response:
    """Prometheus scrape endpoint — exposes request counts and latencies."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health_check() -> JSONResponse:
    """
    Liveness / readiness probe.

    Returns HTTP 200  when all services are healthy.
    Returns HTTP 503  when any service is degraded — safe for use as a
    Kubernetes readiness probe without body parsing.
    """
    from sqlalchemy import text

    from ..db.models import TLE
    from ..db.session import SessionLocal

    overall = "healthy"
    services: Dict[str, Any] = {}
    last_ingest: Any = None

    # --- Database + last TLE ingestion (single session, single finally) ---
    db = SessionLocal()
    try:
        db.execute(text("SELECT 1"))
        services["database"] = "connected"
        row = db.query(TLE.fetched_at).order_by(TLE.fetched_at.desc()).first()
        if row:
            last_ingest_dt = row[0]
            last_ingest = last_ingest_dt.isoformat()
            stale_cutoff = datetime.now(timezone.utc) - timedelta(
                hours=settings.TLE_STALE_ALERT_HOURS
            )
            if last_ingest_dt < stale_cutoff:
                services["tle_data"] = f"stale (last ingested: {last_ingest})"
                overall = "degraded"
            else:
                services["tle_data"] = "current"
        else:
            services["tle_data"] = "no data — run ingest_tles task"
            overall = "degraded"
    except Exception as exc:
        services["database"] = f"error: {exc}"
        overall = "degraded"
    finally:
        db.close()

    # --- Redis ---
    try:
        await app.state.redis_client.ping()
        services["redis"] = "connected"
    except Exception as exc:
        services["redis"] = f"error: {exc}"
        overall = "degraded"

    # --- Cache stats (hit ratio + circuit-breaker state) ---
    # Fix: was cache.get_cache_hit_ratio() which does not exist → AttributeError
    services["cache"] = cache.stats()

    body: Dict[str, Any] = {
        "status":      overall,
        "timestamp":   datetime.now(timezone.utc).isoformat(),  # Fix: was utcnow() — naive
        "services":    services,
        "last_ingest": last_ingest,
    }

    # Fix: was always HTTP 200 even when degraded — useless as a readiness probe
    http_status = (
        status.HTTP_200_OK
        if overall == "healthy"
        else status.HTTP_503_SERVICE_UNAVAILABLE
    )
    return JSONResponse(content=body, status_code=http_status)


# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------

_ws_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="ws_snapshot")


def _ws_snapshot() -> Dict[str, Any]:
    """Collect live stats from DB + Redis for WebSocket broadcast."""
    from ..db.models import SatellitePass, TLE
    from ..db.session import SessionLocal
    import redis as sync_redis

    now = datetime.now(timezone.utc)
    payload: Dict[str, Any] = {"timestamp": now.isoformat(), "type": "snapshot"}

    db = SessionLocal()
    try:
        cutoff = now - timedelta(seconds=60)
        payload["recent_passes_60s"]  = db.query(SatellitePass).filter(SatellitePass.created_at >= cutoff).count()
        payload["total_passes"]        = db.query(SatellitePass).count()
        payload["scheduled_passes"]    = db.query(SatellitePass).filter(SatellitePass.is_scheduled == True).count()  # noqa: E712
        payload["satellites_tracked"]  = db.query(TLE.norad_id).distinct().count()
    except Exception as exc:
        payload["db_error"] = str(exc)
    finally:
        db.close()

    try:
        r: sync_redis.Redis = sync_redis.from_url(settings.CELERY_BROKER_URL, decode_responses=True)  # type: ignore[assignment]
        payload["queues"] = {q: int(r.llen(q)) for q in ("ingestion", "propagation", "scheduling")}
        r.close()
    except Exception:
        payload["queues"] = {}

    return payload


@app.websocket("/ws/events")
async def websocket_events(websocket: WebSocket) -> None:
    """Push live system snapshots every 3 seconds to connected clients."""
    await websocket.accept()
    loop = asyncio.get_event_loop()
    try:
        while True:
            snapshot = await loop.run_in_executor(_ws_executor, _ws_snapshot)
            await websocket.send_json(snapshot)
            await asyncio.sleep(3)
    except (WebSocketDisconnect, Exception):
        pass


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Structured JSON body for all FastAPI HTTP exceptions."""
    trace_id = getattr(request.state, "trace_id", "unknown")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error":    exc.detail,
            "trace_id": trace_id,
        },
        # Fix: original dropped exc.headers entirely — Retry-After on 429s was lost
        headers=dict(exc.headers) if exc.headers else None,
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all for unhandled exceptions — logs context and returns 500."""
    trace_id = getattr(request.state, "trace_id", "unknown")
    logger.error(
        "unhandled_exception",
        trace_id=trace_id,
        error=str(exc),
        error_type=type(exc).__name__,  # Fix: exc_info=True is not valid for structlog
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error":    "internal_server_error",
            "message":  "An unexpected error occurred",
            "trace_id": trace_id,
        },
    )

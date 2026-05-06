"""
api/middleware.py — Request logging, API-key authentication, and rate limiting.

Fixes vs. original
------------------
  - TOCTOU race in RateLimiter fixed — zremrangebyscore + zcard + zadd are
    now executed in a single atomic Lua script; two concurrent requests can no
    longer both slip past the limit by reading the same pre-increment count.
  - Same-millisecond ZADD collision fixed — original used str(now) as the
    sorted-set member, so two requests arriving at the same float timestamp
    overwrote each other (one request counted as zero net entries).  Now uses
    uuid4() so every request has a unique member key.
  - Redis errors isolated — Redis I/O exceptions in APIKeyAuth fall back to DB;
    in RateLimiter they fail open with a warning log so a Redis outage does not
    take the API offline.
  - rate_limit_per_min returned from verify() — callers previously received only
    the key_token; the per-key DB limit was silently discarded, making per-key
    rate caps impossible.  verify() now returns Tuple[str, int].
  - Cache value encodes rate limit — "valid:<rate_limit>" so a cache hit on a
    valid key still delivers the correct per-key limit without a DB round-trip.
  - Retry-After added — check_rate_limit returns (allowed, retry_after_seconds)
    so callers can set the Retry-After response header on 429 responses.
  - time.monotonic() replaces time.time() for duration measurement — immune to
    NTP wall-clock adjustments during long-running requests.
  - Prometheus metrics via constructor injection — eliminates the fragile
    deferred circular import from .main that was silently swallowed on failure.
  - Unhandled exceptions return JSON 500 — re-raising from dispatch() produced
    a plain-text Starlette error body; now returns JSONResponse so API clients
    always receive valid JSON regardless of error type.
  - X-Trace-ID header on error path — header is now set in both the success
    response and the JSONResponse returned on exception.
  - Empty API key guard — "" produced a valid SHA-256 hash that could
    theoretically match a stored digest; now rejected with HTTP 400.
  - last_used_at DB update wrapped in try/except with rollback — a commit
    failure no longer leaves the session in a dirty state.
  - Removed redundant duration_s = duration_ms / 1000 intermediate variable.
"""

import hashlib
import time
from datetime import datetime, timezone
from typing import Any, Optional, Tuple
from uuid import uuid4

from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response
from starlette.types import ASGIApp
from structlog import get_logger

logger = get_logger()
# redis.asyncio.Redis stubs type methods as returning ResponseT (a plain type),
# not CoroutineType, so any Protocol with `async def` is structurally
# incompatible at the type level even though the runtime object IS async.
# Using Any here is the standard pragmatic fix for incomplete async stubs.

# ---------------------------------------------------------------------------
# Lua script — atomic sliding-window rate-limit check + record
# ---------------------------------------------------------------------------
# Executes as a single Redis transaction so the count read and the entry write
# are never interleaved with another request's commands (fixes TOCTOU race).
#
# KEYS[1]  redis key             e.g. "ratelimit:<sha256>"
# ARGV[1]  current timestamp     float seconds since epoch, as string
# ARGV[2]  window start          now - 60, as string
# ARGV[3]  max requests          integer, as string
# ARGV[4]  unique member id      uuid4 hex string
#
# Returns: 0 → allowed,  1 → rate-limited
_RATE_LIMIT_LUA = """
local key       = KEYS[1]
local now       = tonumber(ARGV[1])
local win_start = tonumber(ARGV[2])
local limit     = tonumber(ARGV[3])
local member    = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, 0, win_start)
local count = redis.call('ZCARD', key)
if count >= limit then
    return 1
end
redis.call('ZADD', key, now, member)
redis.call('EXPIRE', key, 60)
return 0
"""


# ---------------------------------------------------------------------------
# RequestLoggingMiddleware
# ---------------------------------------------------------------------------

class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Attach a trace_id to every request, log start/completion, and record
    Prometheus metrics.

    Prometheus counters/histograms are injected via the constructor so this
    module never needs to import from api.main (which imports this module).

    Usage in main.py:
        app.add_middleware(
            RequestLoggingMiddleware,
            request_count=REQUEST_COUNT,
            request_latency=REQUEST_LATENCY,
        )
    Pass neither argument (or None) to run without metrics.
    """

    def __init__(
        self,
        app: ASGIApp,
        request_count: Any = None,
        request_latency: Any = None,
    ) -> None:
        super().__init__(app)
        self._request_count   = request_count    # prometheus Counter or None
        self._request_latency = request_latency  # prometheus Histogram or None

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        trace_id = str(uuid4())
        request.state.trace_id = trace_id

        # monotonic clock — immune to NTP wall-clock adjustments
        start = time.monotonic()

        logger.info(
            "request_received",
            trace_id=trace_id,
            method=request.method,
            path=request.url.path,
            client_ip=request.client.host if request.client else None,
        )

        try:
            response = await call_next(request)
        except Exception as exc:
            duration_ms = round((time.monotonic() - start) * 1000, 2)
            logger.error(
                "request_unhandled_exception",
                trace_id=trace_id,
                error=str(exc),
                error_type=type(exc).__name__,
                duration_ms=duration_ms,
            )
            # Return structured JSON so clients always receive valid JSON on 500s
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"detail": "Internal server error"},
                headers={"X-Trace-ID": trace_id},
            )

        duration_s  = time.monotonic() - start
        duration_ms = round(duration_s * 1000, 2)

        logger.info(
            "request_completed",
            trace_id=trace_id,
            status_code=response.status_code,
            duration_ms=duration_ms,
        )

        self._record_metrics(request.method, request.url.path, response.status_code, duration_s)

        # Set on both success and error paths so clients can always correlate logs
        response.headers["X-Trace-ID"] = trace_id
        return response

    def _record_metrics(
        self, method: str, path: str, status_code: int, duration_s: float
    ) -> None:
        if self._request_count is None and self._request_latency is None:
            return
        try:
            if self._request_count is not None:
                self._request_count.labels(method, path, status_code).inc()
            if self._request_latency is not None:
                self._request_latency.labels(method, path).observe(duration_s)
        except Exception:
            pass  # Metrics must never crash a request


# ---------------------------------------------------------------------------
# APIKeyAuth
# ---------------------------------------------------------------------------

class APIKeyAuth:
    """
    API key authentication with Redis caching.

    Redis cache states for  api_key:<sha256>:
      "valid:<rate_limit>"  key is active (TTL 300 s); rate_limit encoded inline
      "invalid"             key not found or inactive (TTL 60 s)
      "revoked"             key explicitly revoked (TTL 3600 s)

    On Redis failure the lookup falls through to the DB so a Redis outage
    does not lock out all API clients.
    """

    def __init__(self, redis_client: Any) -> None:
        self.redis_client = redis_client

    async def verify(self, api_key: str, db: Session) -> Tuple[str, int]:
        """
        Verify an API key.

        Returns:
            (key_token, rate_limit_per_min)  on success
        Raises:
            HTTPException 400  if api_key is empty
            HTTPException 401  if the key is missing, inactive, or revoked
        """
        if not api_key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="API key must not be empty",
            )

        key_token = hashlib.sha256(api_key.encode()).hexdigest()
        cache_key = f"api_key:{key_token}"

        # --- Redis cache lookup (fail-open: fall through to DB on error) ---
        cached: Optional[str] = None
        try:
            raw = await self.redis_client.get(cache_key)
            if isinstance(raw, str):
                cached = raw
            elif raw is not None:
                # bytes / bytearray / memoryview — all convert via bytes()
                cached = bytes(raw).decode()
        except Exception as exc:
            logger.warning(
                "redis_unavailable_auth_fallback_to_db",
                error=str(exc),
                key_hash_prefix=key_token[:8] + "...",
            )

        if cached in ("revoked", "invalid"):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or revoked API key",
            )

        if cached is not None and cached.startswith("valid:"):
            try:
                rate_limit = int(cached.split(":", 1)[1])
                return key_token, rate_limit
            except (ValueError, IndexError):
                pass  # Malformed cache value — fall through to DB

        # --- DB lookup ---
        from ..db.models import APIKey

        db_key = (
            db.query(APIKey)
            .filter(
                APIKey.key_hash == key_token,
                APIKey.is_active == True,  # noqa: E712
            )
            .first()
        )

        if not db_key:
            try:
                await self.redis_client.setex(cache_key, 60, "invalid")
            except Exception:
                pass
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key",
            )

        rate_limit: int = db_key.rate_limit_per_min

        # Update last_used_at; rollback and continue on failure (non-critical)
        try:
            db_key.last_used_at = datetime.now(timezone.utc)
            db.commit()
        except Exception as exc:
            db.rollback()
            logger.warning("last_used_at_update_failed", error=str(exc))

        # Cache encodes rate limit so cache hits never need a DB round-trip
        try:
            await self.redis_client.setex(cache_key, 300, f"valid:{rate_limit}")
        except Exception:
            pass  # Cache write failure is non-fatal

        return key_token, rate_limit


# ---------------------------------------------------------------------------
# RateLimiter
# ---------------------------------------------------------------------------

class RateLimiter:
    """
    Sliding-window rate limiter backed by a Redis sorted set.

    The entire check-and-record sequence runs as a single Lua script so the
    window count read and the new entry write are atomic — eliminating the
    race condition in the original three-command implementation.

    Each request is recorded with a uuid4 member so two requests arriving at
    the exact same float timestamp produce two distinct sorted-set entries
    instead of overwriting each other.
    """

    def __init__(self, redis_client: Any) -> None:
        self.redis_client = redis_client

    async def check_rate_limit(
        self,
        key_hash: str,
        requests_per_min: int,
    ) -> Tuple[bool, Optional[int]]:
        """
        Atomically check and record a request in the sliding window.

        Returns:
            (True,  None)  — request is within the limit; proceed
            (False, 60)    — limit exceeded; Retry-After = 60 seconds
            (True,  None)  — Redis unavailable; fail-open so Redis outage
                             does not block all traffic
        """
        now          = datetime.now(timezone.utc).timestamp()
        window_start = now - 60.0
        redis_key    = f"ratelimit:{key_hash}"
        member       = str(uuid4())  # unique per request — no same-timestamp collision

        try:
            result = await self.redis_client.eval(
                _RATE_LIMIT_LUA,
                1,                        # numkeys
                redis_key,                # KEYS[1]
                str(now),                 # ARGV[1] — current timestamp
                str(window_start),        # ARGV[2] — window start
                str(requests_per_min),    # ARGV[3] — limit
                member,                   # ARGV[4] — unique member
            )
            if result == 1:
                return False, 60
            return True, None

        except Exception as exc:
            logger.warning(
                "redis_unavailable_rate_limit_fail_open",
                key_hash_prefix=key_hash[:8] + "...",
                error=str(exc),
            )
            # Fail open: allow the request rather than blocking traffic on Redis outage
            return True, None

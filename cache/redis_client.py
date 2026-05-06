"""
Redis cache client — production-grade async cache for satellite pass and TLE data.

Design
------
Connection pool   : Explicit pool size, socket timeouts, TCP keepalive, and
                    periodic health checks prevent stale connections in
                    containerised / Kubernetes environments.

Circuit breaker   : After _MAX_FAILURES consecutive Redis errors the client
                    enters OPEN state and returns None / False immediately for
                    _OPEN_DURATION_SEC seconds.  This stops request pile-ups
                    during Redis downtime from cascading into API timeouts.

Retry             : Transient errors (ConnectionError, TimeoutError) are retried
                    up to _MAX_RETRIES times with exponential back-off before the
                    circuit breaker counts them as a failure.

SCAN instead of KEYS : Pattern-based invalidation uses SCAN (cursor-based, O(1)
                    per page) not KEYS (O(N) over ALL keys, blocks Redis server).
                    On a production instance with millions of keys, KEYS causes
                    multi-second server stalls; SCAN is safe at any scale.

In-memory stats   : Hit / miss counters live in Python memory to avoid an extra
                    Redis round-trip on every cache operation.  They reset on
                    process restart — use Prometheus for durable observability.

Key namespace     : All keys are prefixed with a configurable namespace so dev,
                    staging, and prod can safely share one Redis instance.

JSON safety       : Deserialization errors (corrupt cache entries) are caught,
                    the offending key is deleted, and the call degrades to a
                    cache miss instead of raising into API request handlers.
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnError
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError
from structlog import get_logger

logger = get_logger()

# ---------------------------------------------------------------------------
# Tuning constants — override via subclass or config injection if needed
# ---------------------------------------------------------------------------
_POOL_SIZE: int          = 20     # max concurrent connections per process
_SOCK_TIMEOUT: float     = 5.0   # seconds before a Redis command times out
_CONN_TIMEOUT: float     = 2.0   # seconds before a new connection times out
_HEALTH_INTERVAL: int    = 30    # seconds between automatic keepalive pings
_MAX_RETRIES: int        = 3     # per-operation retry attempts on transient errors
_RETRY_BASE_SEC: float   = 0.1   # base delay; doubles each retry (0.1s, 0.2s, 0.4s)
_MAX_FAILURES: int       = 5     # consecutive failures before circuit opens
_OPEN_DURATION_SEC: float = 30.0 # seconds the circuit stays open before retrying
_SCAN_PAGE: int          = 200   # SCAN cursor page size for key iteration


class RedisCache:
    """
    Async Redis cache with connection pooling, circuit breaker, and retry.

    Preferred usage — async context manager:

        async with RedisCache(redis_url) as cache:
            await cache.set_passes(station_id, query_date, data, ttl=300)
            result = await cache.get_passes(station_id, query_date)

    All public methods return safe values (None / False / {}) on Redis errors
    so a Redis outage degrades to slower DB queries, never to 500 errors.
    """

    def __init__(
        self,
        redis_url: str,
        key_prefix: str = "sda",
        pool_size: int  = _POOL_SIZE,
    ) -> None:
        self._url       = redis_url
        self._prefix    = key_prefix
        self._pool_size = pool_size

        # Typed as non-optional after connect() — accessed only via self._conn
        self._client: Optional[Redis] = None

        # Circuit-breaker state (single event-loop, no locking needed)
        self._failure_count: int   = 0
        self._open_until:    float = 0.0   # monotonic timestamp

        # In-memory hit / miss counters (no extra round-trips)
        self._hits:   int = 0
        self._misses: int = 0

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    async def connect(self) -> None:
        """
        Create the connection pool and verify reachability with PING.

        from_url() is lazy — without PING the first actual command could fail
        and the failure would be attributed to the cache miss handler rather
        than the startup phase, making the root cause harder to diagnose.
        """
        self._client = aioredis.from_url(
            self._url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=self._pool_size,
            socket_timeout=_SOCK_TIMEOUT,
            socket_connect_timeout=_CONN_TIMEOUT,
            socket_keepalive=True,
            retry_on_timeout=True,
            health_check_interval=_HEALTH_INTERVAL,
        )
        # Raises RedisError if the server is unreachable
        await self._client.ping()
        logger.info(
            "Redis cache connected",
            url=self._url,
            pool_size=self._pool_size,
        )

    async def disconnect(self) -> None:
        """Close all pooled connections gracefully."""
        if self._client is not None:
            await self._client.aclose()   # aclose() is the correct async close
            self._client = None
            logger.info("Redis cache disconnected")

    async def __aenter__(self) -> "RedisCache":
        await self.connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.disconnect()

    # -----------------------------------------------------------------------
    # Internal: type-safe client access
    # -----------------------------------------------------------------------

    @property
    def _conn(self) -> Redis:
        """
        Return the active Redis client.

        Raises RuntimeError if connect() was never called.  This gives a clear
        error message instead of an AttributeError on NoneType, which is the
        root cause of all 12 Pylance 'not a known attribute of None' errors in
        the original code.
        """
        if self._client is None:
            raise RuntimeError(
                "RedisCache.connect() must be awaited before issuing commands."
            )
        return self._client

    @property
    def is_connected(self) -> bool:
        """True if connect() has been called and disconnect() has not."""
        return self._client is not None

    # -----------------------------------------------------------------------
    # Key construction
    # -----------------------------------------------------------------------

    def _key(self, *parts: str) -> str:
        """Build a namespaced key: {prefix}:{part1}:{part2}:…"""
        return ":".join([self._prefix, *parts])

    def _pass_key(self, station_id: str, query_date: date) -> str:
        # station_id is String(10) in the DB (e.g. "GS-01"), NOT an int
        return self._key("passes", station_id, query_date.isoformat())

    def _tle_key(self, norad_id: int, epoch: datetime) -> str:
        return self._key("tle", str(norad_id), epoch.strftime("%Y%m%d_%H%M%S"))

    # -----------------------------------------------------------------------
    # Circuit breaker
    # -----------------------------------------------------------------------

    def _circuit_is_open(self) -> bool:
        """True while we are in the fail-fast window."""
        return bool(self._open_until) and time.monotonic() < self._open_until

    def _on_success(self) -> None:
        self._failure_count = 0
        self._open_until    = 0.0

    def _on_failure(self) -> None:
        self._failure_count += 1
        if self._failure_count >= _MAX_FAILURES:
            self._open_until = time.monotonic() + _OPEN_DURATION_SEC
            logger.error(
                "Redis circuit breaker OPEN",
                failures=self._failure_count,
                open_for_sec=_OPEN_DURATION_SEC,
            )

    # -----------------------------------------------------------------------
    # Retry wrapper — core of all Redis calls
    # -----------------------------------------------------------------------

    async def _call(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """
        Execute a Redis command with exponential-back-off retry.

        Transient errors (ConnectionError, TimeoutError) are retried up to
        _MAX_RETRIES times.  Permanent errors and open-circuit state raise
        RedisError immediately so the caller can return a safe fallback.

        All state-mutating calls go through this method so circuit-breaker
        accounting is centralised and can never be bypassed.
        """
        if self._circuit_is_open():
            raise RedisError(
                "Redis circuit breaker is OPEN — failing fast. "
                f"Will retry after {self._open_until - time.monotonic():.0f}s."
            )

        last_exc: Optional[Exception] = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                result = await fn(*args, **kwargs)
                self._on_success()
                return result
            except (RedisConnError, RedisTimeoutError) as exc:
                last_exc = exc
                delay = _RETRY_BASE_SEC * (2 ** (attempt - 1))
                logger.warning(
                    "Redis transient error — retrying",
                    attempt=attempt,
                    of=_MAX_RETRIES,
                    delay_sec=round(delay, 3),
                    error=str(exc),
                )
                await asyncio.sleep(delay)
            except RedisError as exc:
                # Permanent error — count immediately, do not retry
                self._on_failure()
                raise

        # All retries exhausted
        self._on_failure()
        raise RedisError(
            f"Redis command failed after {_MAX_RETRIES} retries"
        ) from last_exc

    # -----------------------------------------------------------------------
    # Pass cache
    # -----------------------------------------------------------------------

    async def get_passes(
        self,
        station_id: str,
        query_date: date,
    ) -> Optional[List[Dict]]:
        """
        Return cached passes, or None on cache miss / Redis error.

        Never raises — a Redis failure degrades gracefully to a DB query.
        Corrupt entries (JSONDecodeError) are deleted and treated as a miss.
        """
        key = self._pass_key(station_id, query_date)
        try:
            raw: Optional[str] = await self._call(self._conn.get, key)
        except RedisError as exc:
            logger.warning("get_passes failed", key=key, error=str(exc))
            self._misses += 1
            return None

        if raw is None:
            self._misses += 1
            logger.debug("Cache miss", key=key)
            return None

        try:
            data: List[Dict] = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Corrupt pass cache entry — evicting", key=key)
            await self._safe_delete(key)
            self._misses += 1
            return None

        self._hits += 1
        logger.debug("Cache hit", key=key, entries=len(data))
        return data

    async def set_passes(
        self,
        station_id: str,
        query_date: date,
        passes: List[Dict],
        ttl_sec: int,
    ) -> bool:
        """
        Cache passes with TTL.  Returns True on success, False on error.

        Serialization failures are logged and return False — they must not
        propagate into the API response handler.
        """
        key = self._pass_key(station_id, query_date)
        try:
            serialized = json.dumps(passes, default=_json_default)
        except (TypeError, ValueError) as exc:
            logger.error(
                "Pass serialization failed — cache write skipped",
                key=key, error=str(exc),
            )
            return False

        try:
            await self._call(self._conn.setex, key, ttl_sec, serialized)
            logger.debug("Cache set", key=key, ttl_sec=ttl_sec, entries=len(passes))
            return True
        except RedisError as exc:
            logger.warning("set_passes failed", key=key, error=str(exc))
            return False

    async def invalidate_station_passes(self, station_id: str) -> int:
        """
        Delete all cached pass entries for one station.

        Uses SCAN (non-blocking, O(1) per page) instead of KEYS (O(N) over all
        keys, blocks the Redis server for the full scan duration — unsafe on a
        production instance with millions of keys).

        Returns the number of keys deleted (0 on error).
        """
        pattern = self._key("passes", station_id, "*")
        try:
            deleted = await self._scan_delete(pattern)
            if deleted:
                logger.info(
                    "Pass cache invalidated",
                    station_id=station_id,
                    keys_deleted=deleted,
                )
            return deleted
        except RedisError as exc:
            logger.warning(
                "invalidate_station_passes failed",
                station_id=station_id,
                error=str(exc),
            )
            return 0

    async def invalidate_all_passes(self) -> int:
        """Delete every cached pass entry across all stations."""
        pattern = self._key("passes", "*")
        try:
            return await self._scan_delete(pattern)
        except RedisError as exc:
            logger.warning("invalidate_all_passes failed", error=str(exc))
            return 0

    # -----------------------------------------------------------------------
    # TLE cache
    # -----------------------------------------------------------------------

    async def get_tle(
        self,
        norad_id: int,
        epoch: datetime,
    ) -> Optional[Dict]:
        """Return a cached TLE dict, or None on miss / error."""
        key = self._tle_key(norad_id, epoch)
        try:
            raw: Optional[str] = await self._call(self._conn.get, key)
        except RedisError as exc:
            logger.warning("get_tle failed", key=key, error=str(exc))
            self._misses += 1
            return None

        if raw is None:
            self._misses += 1
            return None

        try:
            self._hits += 1
            return json.loads(raw)  # type: ignore[return-value]
        except json.JSONDecodeError:
            logger.warning("Corrupt TLE cache entry — evicting", key=key)
            await self._safe_delete(key)
            self._misses += 1
            return None

    async def set_tle(
        self,
        norad_id: int,
        epoch: datetime,
        tle_data: Dict,
        ttl_sec: int,
    ) -> bool:
        """Cache a TLE record with TTL.  Returns True on success."""
        key = self._tle_key(norad_id, epoch)
        try:
            payload = json.dumps(tle_data, default=_json_default)
            await self._call(self._conn.setex, key, ttl_sec, payload)
            return True
        except (RedisError, TypeError, ValueError) as exc:
            logger.warning("set_tle failed", key=key, error=str(exc))
            return False

    # -----------------------------------------------------------------------
    # Health & observability
    # -----------------------------------------------------------------------

    async def ping(self) -> bool:
        """Return True if Redis responds to PING within the socket timeout."""
        try:
            await self._call(self._conn.ping)
            return True
        except RedisError:
            return False

    def hit_ratio(self) -> float:
        """In-process cache hit ratio since last process start [0.0 – 1.0]."""
        total = self._hits + self._misses
        return self._hits / total if total else 0.0

    def stats(self) -> Dict[str, Any]:
        """Return a snapshot of cache counters and circuit-breaker state."""
        total = self._hits + self._misses
        return {
            "hits":           self._hits,
            "misses":         self._misses,
            "total_requests": total,
            "hit_ratio":      round(self.hit_ratio(), 4),
            "circuit_open":   self._circuit_is_open(),
            "failure_count":  self._failure_count,
        }

    async def server_info(self) -> Dict[str, Any]:
        """
        Return key Redis server metrics for a /health endpoint.

        Safe to call in readiness probes — returns {} on error.
        """
        try:
            info: Dict = await self._call(self._conn.info, "all")
            return {
                "used_memory_human":        info.get("used_memory_human"),
                "connected_clients":        info.get("connected_clients"),
                "total_commands_processed": info.get("total_commands_processed"),
                "keyspace_hits":            info.get("keyspace_hits"),
                "keyspace_misses":          info.get("keyspace_misses"),
                "uptime_in_seconds":        info.get("uptime_in_seconds"),
            }
        except RedisError as exc:
            logger.warning("server_info failed", error=str(exc))
            return {}

    # -----------------------------------------------------------------------
    # Private utilities
    # -----------------------------------------------------------------------

    async def _scan_delete(self, pattern: str) -> int:
        """
        Delete all keys matching `pattern` via SCAN cursor iteration.

        Each SCAN page fetches at most _SCAN_PAGE keys and yields control back
        to the event loop, so even massive key sets do not block the server or
        the asyncio event loop.
        """
        deleted = 0
        cursor  = 0
        while True:
            cursor, keys = await self._call(
                self._conn.scan, cursor, match=pattern, count=_SCAN_PAGE
            )
            if keys:
                await self._call(self._conn.delete, *keys)
                deleted += len(keys)
            if cursor == 0:
                break
        return deleted

    async def _safe_delete(self, key: str) -> None:
        """Delete a single key, swallowing all Redis errors."""
        try:
            await self._call(self._conn.delete, key)
        except RedisError:
            pass


# ---------------------------------------------------------------------------
# JSON serializer
# ---------------------------------------------------------------------------

def _json_default(obj: Any) -> str:
    """
    Extend json.dumps for types the standard encoder cannot handle.

    datetime → ISO-8601 string with UTC tzinfo (timezone-naive datetimes are
               labelled as UTC, matching our DB convention).
    date     → ISO-8601 date string.
    Anything else raises TypeError so the caller sees the failure rather than
    silently getting a wrong serialization.
    """
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(
        f"Object of type {type(obj).__name__!r} is not JSON serializable"
    )

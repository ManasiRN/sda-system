"""
workers/celery_app.py — Celery task definitions for the SDA pipeline.

Fixes vs. previous version
---------------------------
  - run_greedy flooding fixed — detect_passes called run_greedy.delay() on
    every completion; with 15 000+ satellites this queued 15 000+ greedy runs.
    Now uses a Redis atomic DECR counter (keyed per batch) so run_greedy fires
    exactly once after the last detect_passes in that batch completes.
    Backward-compatible: calls without a counter_key (manual triggers) still
    fire run_greedy directly.
  - broker visibility_timeout added — without this the Redis broker re-delivers
    any task that takes longer than the default 1 h, even if the worker is still
    processing it, causing duplicate propagation work.  Set to 2 h to exceed the
    per-task hard limit.
  - broker_heartbeat added — detects stalled broker TCP connections every 10 s
    so workers reconnect quickly instead of hanging indefinitely.
  - worker_lost_wait added — declares a worker lost after 10 s of silence
    instead of waiting forever before re-delivering its in-progress tasks.
  - SoftTimeLimitExceeded handled in detect_passes — a timed-out task now
    rolls back cleanly, decrements the counter so greedy still fires, and does
    NOT retry (retrying a timed-out propagation task risks re-queueing a task
    that will always time out, looping until max_retries).
  - Per-task time limits — each task type gets its own time_limit /
    soft_time_limit appropriate to its expected runtime rather than inheriting
    the global 1 h default.  ingest_tles: 5 min.  propagate_batch: 10 min.
    detect_passes: 15 min.  run_greedy / run_ortools: 2 min.
  - Chunked dispatch logging in propagate_batch — large fan-outs are logged
    every _PROPAGATION_CHUNK_SIZE satellites so operators can see progress
    without waiting for all 15 000+ .delay() calls to complete silently.
  - check_queue_depth error-isolated — Redis unavailability no longer crashes
    the monitoring task; failures are logged and an empty result is returned.

Previous fixes (still in place)
--------------------------------
  - OOM risk fixed — detect_passes flushes to DB after each TLE.
  - asyncio.run() replaces manual event loop management.
  - kombu.Queue objects used in task_queues.
  - MaxRetriesExceededError handled in every task.
  - max_retries=3 on run_greedy and run_ortools.
  - db.rollback() in all except blocks.
  - check_queue_depth uses Redis LLEN directly.
  - Exponential back-off capped at 300 s.
  - raise self.retry(...) used consistently.
  - broker_connection_retry_on_startup=True.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, cast

import redis as sync_redis
from celery import Celery
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded
from celery.schedules import crontab
from celery.signals import task_failure
from kombu import Queue
from sqlalchemy import Table
from sqlalchemy.orm import Session
from structlog import get_logger

from ..config import config
from ..db.models import TLE, GroundStation, SatellitePass
from ..db.session import SessionLocal
from ..ingestion.fetcher import TLEFetcher
from ..propagation.pass_detector import pass_detector
from ..propagation.sgp4_engine import PropagationResult, sgp4_engine
from ..scheduling.greedy import GreedyScheduler
from ..scheduling.ortools_optimizer import ORToolsOptimizer

logger = get_logger()

# ---------------------------------------------------------------------------
# Celery application
# ---------------------------------------------------------------------------

app = Celery(
    "sda_system",
    broker=config.CELERY_BROKER_URL,
    backend=config.CELERY_RESULT_BACKEND,
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    # Global fallback limits — each task overrides these via its decorator.
    task_time_limit=3600,
    task_soft_time_limit=3000,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=100,
    result_expires=86400,
    broker_connection_retry_on_startup=True,
    # Heartbeat: detect stalled broker connections every 10 s.
    broker_heartbeat=10,
    # visibility_timeout must exceed the longest task's hard time_limit.
    # Without this, Redis re-delivers in-progress propagation tasks after 1 h
    # even though the worker is still running them, causing duplicate work.
    broker_transport_options={
        "visibility_timeout": 7200,      # 2 h — exceeds detect_passes hard limit
        "socket_timeout": 30,
        "socket_connect_timeout": 10,
    },
    # Declare a worker lost after 10 s of silence before re-delivering tasks.
    worker_lost_wait=10.0,
    task_queues=(
        Queue("ingestion"),
        Queue("propagation"),
        Queue("scheduling"),
        Queue("errors"),
    ),
    task_routes={
        "sda_system.workers.celery_app.ingest_tles":        {"queue": "ingestion"},
        "sda_system.workers.celery_app.propagate_batch":    {"queue": "propagation"},
        "sda_system.workers.celery_app.detect_passes":      {"queue": "propagation"},
        "sda_system.workers.celery_app.run_greedy":         {"queue": "scheduling"},
        "sda_system.workers.celery_app.run_ortools":        {"queue": "scheduling"},
        "sda_system.workers.celery_app.check_queue_depth":  {"queue": "ingestion"},
        "sda_system.workers.celery_app.record_dead_letter": {"queue": "errors"},
    },
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_RETRY_COUNTDOWN    = 300    # cap exponential back-off at 5 minutes
_DEPTH_THRESHOLD        = 1000   # warn when total pending tasks exceed this
_QUEUE_NAMES            = ("ingestion", "propagation", "scheduling")
_PROPAGATION_CHUNK_SIZE = 500    # log a progress line every N dispatched tasks
_PROPAGATE_COUNTER_KEY  = "sda:propagate_remaining"
_PROPAGATE_COUNTER_TTL  = 86400  # 24 h — auto-expire stale counters


def _countdown(retries: int) -> int:
    """Exponential back-off capped at _MAX_RETRY_COUNTDOWN, minimum 2 s."""
    return min(_MAX_RETRY_COUNTDOWN, max(2, 2 ** retries))


# ---------------------------------------------------------------------------
# Private Redis helper
# ---------------------------------------------------------------------------

def _redis_client() -> sync_redis.Redis:
    return sync_redis.from_url(  # type: ignore[return-value]
        config.CELERY_BROKER_URL, decode_responses=True
    )


def _propagate_counter_decr(batch_key: str) -> None:
    """
    Atomically decrement the per-batch propagation counter.

    Triggers run_greedy exactly once when the counter reaches zero —
    i.e. when every detect_passes task in the batch has finished (success,
    timeout, or permanent failure).

    If batch_key is empty the call came from a manual (non-batched) trigger;
    run_greedy is fired directly to preserve the original single-satellite
    behaviour.
    """
    if not batch_key:
        # Manual / legacy invocation — fire greedy unconditionally.
        run_greedy.delay()  # type: ignore[attr-defined]
        return

    r = _redis_client()
    try:
        remaining = cast(int, r.decr(batch_key))
        if remaining <= 0:
            logger.info(
                "propagate_counter_zero_triggering_greedy",
                batch_key=batch_key,
                remaining=remaining,
            )
            run_greedy.delay()  # type: ignore[attr-defined]
    except Exception as exc:
        logger.warning("propagate_counter_decr_failed", batch_key=batch_key, error=str(exc))
    finally:
        r.close()


# ---------------------------------------------------------------------------
# Private DB helper
# ---------------------------------------------------------------------------

def _bulk_upsert_passes(db: Session, rows: List[Dict]) -> None:
    """
    Batch-insert pass rows with ON CONFLICT DO NOTHING.

    Idempotent: re-running after a retry silently skips rows already committed.
    """
    if not rows:
        return
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    _table: Table = cast(Table, SatellitePass.__table__)
    stmt = pg_insert(_table).values(rows)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["norad_id", "station_id", "rise_time"]
    )
    db.execute(stmt)
    db.commit()


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@app.task(
    bind=True, max_retries=3, queue="ingestion",
    time_limit=300, soft_time_limit=240,  # 5 min hard / 4 min soft
)
def ingest_tles(self) -> Dict[str, Any]:
    """Fetch and ingest TLEs from Celestrak, then kick off propagation."""
    logger.info("ingest_tles_started")

    db = SessionLocal()
    try:
        fetcher = TLEFetcher(db)
        fetcher.init_ground_stations()
        result = asyncio.run(fetcher.fetch_all())
        logger.info("ingest_tles_completed", result=result)
        propagate_batch.delay([])  # type: ignore[attr-defined]
        return result

    except Exception as exc:
        db.rollback()
        logger.error("ingest_tles_failed", error=str(exc), retries=self.request.retries)
        try:
            raise self.retry(exc=exc, countdown=_countdown(self.request.retries))
        except MaxRetriesExceededError:
            logger.error("ingest_tles_max_retries_exceeded", error=str(exc))
            raise
    finally:
        db.close()


@app.task(
    bind=True, max_retries=3, queue="propagation",
    time_limit=600, soft_time_limit=540,  # 10 min hard / 9 min soft
)
def propagate_batch(self, norad_ids: List[int]) -> Dict[str, Any]:
    """
    Resolve the satellite list, set the completion counter, and dispatch
    one detect_passes task per satellite.

    A unique batch_key is created for this invocation so that parallel or
    sequential runs do not share a counter and interfere with each other.
    The counter is set BEFORE dispatching tasks to eliminate the race where
    the last task completes before the counter is initialised.
    """
    logger.info("propagate_batch_started", satellite_count=len(norad_ids))

    db = SessionLocal()
    try:
        if not norad_ids:
            norad_ids = [n[0] for n in db.query(TLE.norad_id).distinct().all()]
    except Exception as exc:
        db.rollback()
        logger.error("propagate_batch_failed", error=str(exc), retries=self.request.retries)
        try:
            raise self.retry(exc=exc, countdown=_countdown(self.request.retries))
        except MaxRetriesExceededError:
            logger.error("propagate_batch_max_retries_exceeded", error=str(exc))
            raise
    finally:
        db.close()

    if not norad_ids:
        logger.warning("propagate_batch_no_satellites")
        return {"dispatched": 0}

    # Unique key per batch — prevents old tasks from a previous run
    # decrementing this batch's counter.
    batch_key = (
        f"{_PROPAGATE_COUNTER_KEY}"
        f":{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
        f":{self.request.id or 'manual'}"
    )

    r = _redis_client()
    try:
        r.set(batch_key, len(norad_ids), ex=_PROPAGATE_COUNTER_TTL)
    except Exception as exc:
        logger.warning("propagate_counter_set_failed", error=str(exc))
        batch_key = ""  # fall back to old fire-every-time behaviour
    finally:
        r.close()

    for i in range(0, len(norad_ids), _PROPAGATION_CHUNK_SIZE):
        chunk = norad_ids[i : i + _PROPAGATION_CHUNK_SIZE]
        for norad_id in chunk:
            detect_passes.apply_async(  # type: ignore[attr-defined]
                args=([norad_id],),
                kwargs={"counter_key": batch_key},
            )
        logger.info(
            "propagate_chunk_dispatched",
            offset=i,
            chunk_size=len(chunk),
            total=len(norad_ids),
        )

    logger.info("propagate_batch_dispatched", satellites=len(norad_ids))
    return {"dispatched": len(norad_ids)}


@app.task(
    bind=True, max_retries=3, queue="propagation",
    time_limit=900, soft_time_limit=780,  # 15 min hard / 13 min soft
)
def detect_passes(
    self, norad_ids: List[int], counter_key: str = ""
) -> Dict[str, Any]:
    """
    Propagate satellites via SGP4 and detect passes over all ground stations.

    counter_key: opaque Redis key set by propagate_batch; when this task
    completes (success, timeout, or permanent failure) it decrements the
    counter and triggers run_greedy if the counter reaches zero.  Empty
    string means a manual invocation — run_greedy fires unconditionally.
    """
    logger.info("detect_passes_started", satellite_count=len(norad_ids))

    db = SessionLocal()
    try:
        if not norad_ids:
            norad_ids = [n[0] for n in db.query(TLE.norad_id).distinct().all()]

        tles = (
            db.query(TLE)
            .filter(TLE.norad_id.in_(norad_ids), TLE.is_current == True)  # noqa: E712
            .all()
        )
        stations = (
            db.query(GroundStation)
            .filter(GroundStation.is_active == True)  # noqa: E712
            .all()
        )

        if not stations:
            logger.warning("detect_passes_no_stations")
            _propagate_counter_decr(counter_key)
            return {"passes_detected": 0}

        start_time   = datetime.now(timezone.utc)
        total_passes = 0

        for tle in tles:
            try:
                result: PropagationResult = sgp4_engine.propagate(
                    line1=tle.line1,
                    line2=tle.line2,
                    norad_id=tle.norad_id,
                    start_time=start_time,
                    name=tle.name or "",
                )
            except Exception as exc:
                logger.error("propagation_failed", norad_id=tle.norad_id, error=str(exc))
                continue

            if not result.is_usable():
                logger.warning(
                    "propagation_not_usable",
                    norad_id=tle.norad_id,
                    valid_pct=f"{result.valid_fraction * 100:.1f}%",
                    decayed=result.decayed,
                )
                continue

            valid_positions = result.valid_positions
            valid_times     = result.valid_times
            if len(valid_positions) == 0:
                continue

            tle_rows: List[Dict] = []
            for station in stations:
                station_dict = {
                    "id":                 station.station_id,
                    "latitude":           station.latitude,
                    "longitude":          station.longitude,
                    "altitude_m":         station.altitude_m,
                    "elevation_mask_deg": getattr(
                        station, "elevation_mask_deg", config.MIN_ELEVATION_DEG
                    ),
                }
                try:
                    detected = pass_detector.detect_passes(
                        valid_positions, valid_times, station_dict
                    )
                except Exception as exc:
                    logger.error(
                        "pass_detection_failed",
                        norad_id=tle.norad_id,
                        station=station.station_id,
                        error=str(exc),
                    )
                    continue

                for p in detected:
                    tle_rows.append({
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

            _bulk_upsert_passes(db, tle_rows)
            total_passes += len(tle_rows)

        logger.info("detect_passes_completed", passes_detected=total_passes)
        _propagate_counter_decr(counter_key)
        return {"passes_detected": total_passes}

    except SoftTimeLimitExceeded:
        # Roll back any in-progress DB write, then decrement so greedy still
        # fires.  Do NOT retry — a task that always times out would loop until
        # max_retries and waste worker slots on work that cannot complete.
        db.rollback()
        logger.warning(
            "detect_passes_soft_timeout",
            norad_ids=norad_ids,
            counter_key=counter_key,
        )
        _propagate_counter_decr(counter_key)
        raise

    except Exception as exc:
        db.rollback()
        logger.error("detect_passes_failed", error=str(exc), retries=self.request.retries)
        try:
            # counter_key is forwarded through retries so the counter is
            # decremented only once — on final success or MaxRetriesExceededError.
            raise self.retry(
                exc=exc,
                countdown=_countdown(self.request.retries),
                kwargs={"counter_key": counter_key},
            )
        except MaxRetriesExceededError:
            logger.error("detect_passes_max_retries_exceeded", error=str(exc))
            _propagate_counter_decr(counter_key)
            raise

    finally:
        db.close()


@app.task(
    bind=True, max_retries=3, queue="scheduling",
    time_limit=120, soft_time_limit=90,  # 2 min hard / 90 s soft
)
def run_greedy(self) -> Dict[str, Any]:
    """Stage 1 — greedy weighted scheduler across all active stations."""
    logger.info("run_greedy_started")

    db = SessionLocal()
    try:
        start_time = datetime.now(timezone.utc)
        end_time   = start_time + timedelta(days=config.PROPAGATION_DAYS)

        scheduler = GreedyScheduler(session=db)
        scheduled, scheduled_sats = scheduler.schedule_all_stations(start_time, end_time)

        stations = (
            db.query(GroundStation)
            .filter(GroundStation.is_active == True)  # noqa: E712
            .all()
        )
        free_slots: Dict[str, List] = {
            station.station_id: scheduler.get_free_slots(
                station.station_id, start_time, end_time
            )
            for station in stations
        }

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(
            "run_greedy_completed",
            scheduled_passes=len(scheduled),
            unique_satellites=len(scheduled_sats),
            duration_sec=round(duration, 2),
        )

        run_ortools.delay(list(scheduled_sats), free_slots)  # type: ignore[attr-defined]
        return {
            "scheduled_passes":  len(scheduled),
            "unique_satellites": len(scheduled_sats),
            "duration_sec":      round(duration, 2),
        }

    except Exception as exc:
        db.rollback()
        logger.error("run_greedy_failed", error=str(exc), retries=self.request.retries)
        try:
            raise self.retry(exc=exc, countdown=_countdown(self.request.retries))
        except MaxRetriesExceededError:
            logger.error("run_greedy_max_retries_exceeded", error=str(exc))
            raise
    finally:
        db.close()


@app.task(
    bind=True, max_retries=3, queue="scheduling",
    time_limit=120, soft_time_limit=90,  # 2 min hard / 90 s soft
)
def run_ortools(self, scheduled_sats: List[int], free_slots: Dict[str, List]) -> Dict[str, Any]:
    """Stage 2 — OR-Tools CP-SAT optimizer for remaining unscheduled satellites."""
    logger.info("run_ortools_started")

    db = SessionLocal()
    try:
        start_time = datetime.now(timezone.utc)
        end_time   = start_time + timedelta(days=config.PROPAGATION_DAYS)

        parsed_slots: Dict[str, List] = {
            station_id: [
                (datetime.fromisoformat(s[0]), datetime.fromisoformat(s[1]))
                for s in slots
            ]
            for station_id, slots in free_slots.items()
        }

        if not parsed_slots:
            scheduler = GreedyScheduler(session=db)
            stations = (
                db.query(GroundStation)
                .filter(GroundStation.is_active == True)  # noqa: E712
                .all()
            )
            raw_slots = {
                s.station_id: scheduler.get_free_slots(s.station_id, start_time, end_time)
                for s in stations
            }
            parsed_slots = {
                sid: [
                    (datetime.fromisoformat(s[0]), datetime.fromisoformat(s[1]))
                    for s in slots
                ]
                for sid, slots in raw_slots.items()
            }
            logger.info("run_ortools_computed_free_slots", stations=len(parsed_slots))

        if not scheduled_sats:
            scheduled_sats = [
                row[0]
                for row in db.query(SatellitePass.norad_id)
                .filter(SatellitePass.is_scheduled == True)  # noqa: E712
                .distinct()
                .all()
            ]
            logger.info("run_ortools_computed_scheduled_sats", count=len(scheduled_sats))

        optimizer = ORToolsOptimizer(
            db,
            time_limit_sec=config.ORTOOLS_TIME_LIMIT_SECONDS,
            num_workers=config.MAX_WORKERS,
        )
        newly_scheduled = optimizer.optimize_remaining(
            set(scheduled_sats), start_time, end_time, parsed_slots
        )

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(
            "run_ortools_completed",
            newly_scheduled_passes=len(newly_scheduled),
            duration_sec=round(duration, 2),
        )
        return {
            "newly_scheduled_passes": len(newly_scheduled),
            "duration_sec":           round(duration, 2),
        }

    except Exception as exc:
        db.rollback()
        logger.error("run_ortools_failed", error=str(exc), retries=self.request.retries)
        try:
            raise self.retry(exc=exc, countdown=_countdown(self.request.retries))
        except MaxRetriesExceededError:
            logger.error("run_ortools_max_retries_exceeded", error=str(exc))
            raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Dead-letter queue
# ---------------------------------------------------------------------------

@app.task(queue="errors", ignore_result=True)
def record_dead_letter(
    task_name: str,
    task_id: str,
    error: str,
    error_type: str,
) -> None:
    logger.error(
        "dead_letter_received",
        task_name=task_name,
        task_id=task_id,
        error=error,
        error_type=error_type,
    )


@task_failure.connect
def _on_task_failure(sender, task_id, exception, traceback, einfo, **kwargs) -> None:
    if isinstance(exception, MaxRetriesExceededError):
        try:
            record_dead_letter.apply_async(  # type: ignore[attr-defined]
                kwargs={
                    "task_name":  sender.name if hasattr(sender, "name") else str(sender),
                    "task_id":    task_id,
                    "error":      str(exception),
                    "error_type": type(exception).__name__,
                },
                queue="errors",
            )
        except Exception as exc:
            logger.error("dead_letter_dispatch_failed", task_id=task_id, error=str(exc))


# ---------------------------------------------------------------------------
# Queue depth monitor
# ---------------------------------------------------------------------------

@app.task(queue="ingestion")
def check_queue_depth() -> Dict[str, Any]:
    """
    Report pending task counts per queue for backpressure alerting.

    Uses Redis LLEN directly — Celery's Redis transport stores each named
    queue as a plain Redis list (LPUSH on enqueue, BRPOP on consume).
    """
    r = _redis_client()
    try:
        per_queue: Dict[str, int] = {
            name: cast(int, r.llen(name)) for name in _QUEUE_NAMES
        }
    except Exception as exc:
        logger.warning("check_queue_depth_redis_error", error=str(exc))
        return {"queue_depth": -1, "per_queue": {}}
    finally:
        r.close()

    total_depth = sum(per_queue.values())
    logger.info("check_queue_depth", total_depth=total_depth, per_queue=per_queue)
    if total_depth > _DEPTH_THRESHOLD:
        logger.warning(
            "queue_depth_threshold_exceeded",
            threshold=_DEPTH_THRESHOLD,
            total_depth=total_depth,
            per_queue=per_queue,
        )
    return {"queue_depth": total_depth, "per_queue": per_queue}


# ---------------------------------------------------------------------------
# Beat schedule
# ---------------------------------------------------------------------------

app.conf.beat_schedule = {
    "ingest-tles-every-6-hours": {
        "task":     "sda_system.workers.celery_app.ingest_tles",
        "schedule": crontab(minute="0", hour="*/6"),
        "options":  {"queue": "ingestion"},
    },
    "check-queue-depth-every-5-minutes": {
        "task":     "sda_system.workers.celery_app.check_queue_depth",
        "schedule": crontab(minute="*/5"),
        "options":  {"queue": "ingestion"},
    },
}

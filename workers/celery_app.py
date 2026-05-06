"""
workers/celery_app.py — Celery task definitions for the SDA pipeline.

Fixes vs. original
------------------
  - OOM risk fixed — detect_passes() accumulated ALL pass rows for all
    satellites × all stations in one in-memory list before a single bulk
    insert.  1 000 satellites × 50 stations × ~10 passes ≈ 500 000 dicts
    (~250 MB) per invocation.  Now flushes to DB after each TLE via
    _bulk_upsert_passes(), keeping peak RAM proportional to one satellite's
    passes only.
  - asyncio.run() replaces manual event loop management — the original used
    new_event_loop() + set_event_loop() + run_until_complete() + close().
    set_event_loop() is not thread-safe; the loop was never closed on
    exception.  asyncio.run() handles all of this correctly.
  - kombu.Queue objects replace plain dicts in task_queues — Celery's
    task_queues config expects Queue instances; plain dicts are accepted
    silently but can break with certain broker transport plugins.
  - MaxRetriesExceededError handled in every task — the original only guarded
    ingest_tles; all other tasks let it propagate unlogged to Celery's default
    error handler with no context about which task or payload failed.
  - max_retries=3 added to run_greedy and run_ortools — was None (unlimited),
    which can cause tasks to loop indefinitely and back up the scheduling queue.
  - db.rollback() added to all except blocks — without rollback a failed DB
    operation left the session in a transaction-aborted state; the next query
    on the same session raised InFailedSqlTransaction instead of the real error.
  - default_retry_delay removed from ingest_tles decorator — every self.retry()
    call already supplies an explicit countdown that overrides default_retry_delay;
    the decorator value was dead, misleading configuration.
  - check_queue_depth rewired to query Redis LLEN directly — the previous
    implementation used inspect.active_queues() which returns queue topology
    (name, exchange, routing key, durable flags) and has NO 'messages' key.
    q.get('messages', 0) always returned 0 so total_depth was always 0 and
    the threshold warning never fired.  Celery's Redis transport stores each
    named queue as a plain Redis list, so LLEN gives the exact pending count.
  - check_queue_depth returns per-queue breakdown — callers and log aggregators
    can now identify WHICH queue is backed up, not just the global total.
  - check_queue_depth added to beat_schedule — the task existed but was never
    scheduled; it only ran if explicitly triggered via celery call.
  - _countdown minimum raised to 2 s — 2**0 = 1 s is too short for a Redis
    broker to recover; max(2, ...) ensures the first retry always waits ≥ 2 s.
  - Exponential back-off capped at 300 s — 2**retries grows without bound;
    min(300, 2**retries) prevents tasks from being delayed by hours on retry 10+.
  - raise self.retry(...) used consistently — all tasks now use the explicit
    raise form so the Retry exception propagates reliably through linters and
    type checkers without appearing as dead code after the call.
  - Unused Base import removed.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, cast

from celery import Celery
from celery.exceptions import MaxRetriesExceededError
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
    task_time_limit=3600,
    task_soft_time_limit=3000,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=100,
    result_expires=86400,
    # Fix: suppress CPendingDeprecationWarning — retry on startup is the correct default
    broker_connection_retry_on_startup=True,
    task_queues=(
        Queue("ingestion"),
        Queue("propagation"),
        Queue("scheduling"),
        Queue("errors"),        # dead-letter queue for permanently-failed tasks
    ),
    task_routes={
        "sda_system.workers.celery_app.ingest_tles":       {"queue": "ingestion"},
        "sda_system.workers.celery_app.propagate_batch":   {"queue": "propagation"},
        "sda_system.workers.celery_app.detect_passes":     {"queue": "propagation"},
        "sda_system.workers.celery_app.run_greedy":        {"queue": "scheduling"},
        "sda_system.workers.celery_app.run_ortools":       {"queue": "scheduling"},
        "sda_system.workers.celery_app.check_queue_depth": {"queue": "ingestion"},
        "sda_system.workers.celery_app.record_dead_letter": {"queue": "errors"},
    },
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_RETRY_COUNTDOWN = 300   # cap exponential back-off at 5 minutes
_DEPTH_THRESHOLD     = 1000  # log warning when total pending tasks exceed this
_QUEUE_NAMES         = ("ingestion", "propagation", "scheduling")


def _countdown(retries: int) -> int:
    """
    Exponential back-off capped at _MAX_RETRY_COUNTDOWN seconds.

    Minimum of 2 s — 2**0 = 1 s is too short for a Redis broker to recover
    from a transient failure before the first retry fires.
    """
    return min(_MAX_RETRY_COUNTDOWN, max(2, 2 ** retries))


# ---------------------------------------------------------------------------
# Private DB helper
# ---------------------------------------------------------------------------

def _bulk_upsert_passes(db: Session, rows: List[Dict]) -> None:
    """
    Batch-insert pass rows with ON CONFLICT DO NOTHING.

    Idempotent: re-running after a retry silently skips rows already committed.
    Importing pg_insert here keeps the module importable on non-PostgreSQL test
    environments that don't run the full pipeline.
    """
    if not rows:
        return
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    # DeclarativeBase.__table__ is statically typed as FromClause; at runtime it
    # is always a Table.  cast() informs the type checker without a runtime cost.
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

@app.task(bind=True, max_retries=3, queue="ingestion")
def ingest_tles(self) -> Dict[str, Any]:
    """Fetch and ingest TLEs from Celestrak, then kick off propagation."""
    logger.info("ingest_tles_started")

    db = SessionLocal()
    try:
        fetcher = TLEFetcher(db)
        fetcher.init_ground_stations()

        # Fix: was new_event_loop() + set_event_loop() + run_until_complete() +
        # close().  set_event_loop() is not thread-safe and the loop was never
        # closed on exception.  asyncio.run() handles all of this correctly.
        result = asyncio.run(fetcher.fetch_all())

        logger.info("ingest_tles_completed", result=result)
        propagate_batch.delay([])  # type: ignore[attr-defined]
        return result

    except Exception as exc:
        db.rollback()  # Fix: leave session clean before retry opens a new one
        logger.error("ingest_tles_failed", error=str(exc), retries=self.request.retries)
        try:
            raise self.retry(exc=exc, countdown=_countdown(self.request.retries))
        except MaxRetriesExceededError:
            logger.error("ingest_tles_max_retries_exceeded", error=str(exc))
            raise
    finally:
        db.close()


@app.task(bind=True, max_retries=3, queue="propagation")
def propagate_batch(self, norad_ids: List[int]) -> Dict[str, Any]:
    """Resolve the satellite list and dispatch pass-detection tasks."""
    logger.info("propagate_batch_started", satellite_count=len(norad_ids))

    db = SessionLocal()
    try:
        if not norad_ids:
            norad_ids = [n[0] for n in db.query(TLE.norad_id).distinct().all()]

        # One task per satellite: enables true parallelism and bounds peak memory.
        # A single detect_passes([id1, ..., id100]) task takes 16+ hours; dispatching
        # one task per satellite lets all workers process them concurrently.
        for norad_id in norad_ids:
            detect_passes.delay([norad_id])  # type: ignore[attr-defined]
        logger.info("propagate_batch_dispatched", satellites=len(norad_ids))
        return {"dispatched": len(norad_ids)}

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


@app.task(bind=True, max_retries=3, queue="propagation")
def detect_passes(self, norad_ids: List[int]) -> Dict[str, Any]:
    """
    Propagate satellites via SGP4 and detect passes over all ground stations.

    Flushes to DB after each TLE to bound peak memory.  The original
    accumulated all rows (sats × stations × passes) before a single bulk
    insert — ~250 MB per invocation for a 1 000-satellite set.
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
            return {"passes_detected": 0}

        start_time   = datetime.now(timezone.utc)
        total_passes = 0

        for tle in tles:
            # --- Propagate this TLE ---
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

            # --- Detect passes for this TLE across all stations ---
            tle_rows: List[Dict] = []
            for station in stations:
                station_dict = {
                    "id":                 station.station_id,
                    "latitude":           station.latitude,
                    "longitude":          station.longitude,
                    "altitude_m":         station.altitude_m,
                    "elevation_mask_deg": getattr(station, "elevation_mask_deg", config.MIN_ELEVATION_DEG),
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

            # Fix: flush per TLE — prevents OOM from accumulating all rows
            _bulk_upsert_passes(db, tle_rows)
            total_passes += len(tle_rows)

        logger.info("detect_passes_completed", passes_detected=total_passes)
        run_greedy.delay()  # type: ignore[attr-defined]
        return {"passes_detected": total_passes}

    except Exception as exc:
        db.rollback()
        logger.error("detect_passes_failed", error=str(exc), retries=self.request.retries)
        try:
            raise self.retry(exc=exc, countdown=_countdown(self.request.retries))
        except MaxRetriesExceededError:
            logger.error("detect_passes_max_retries_exceeded", error=str(exc))
            raise
    finally:
        db.close()


@app.task(bind=True, max_retries=3, queue="scheduling")  # Fix: was missing max_retries
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


@app.task(bind=True, max_retries=3, queue="scheduling")  # Fix: was missing max_retries
def run_ortools(self, scheduled_sats: List[int], free_slots: Dict[str, List]) -> Dict[str, Any]:
    """Stage 2 — OR-Tools CP-SAT optimizer for remaining unscheduled satellites."""
    logger.info("run_ortools_started")

    db = SessionLocal()
    try:
        start_time = datetime.now(timezone.utc)
        end_time   = start_time + timedelta(days=config.PROPAGATION_DAYS)

        # JSON serializes tuples as lists; s[0]/s[1] indexing handles both forms
        parsed_slots: Dict[str, List] = {
            station_id: [
                (datetime.fromisoformat(s[0]), datetime.fromisoformat(s[1]))
                for s in slots
            ]
            for station_id, slots in free_slots.items()
        }

        # If free_slots was empty (e.g. triggered manually without greedy first),
        # derive the current free windows from the DB so OR-Tools has valid slots.
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
            logger.info(
                "run_ortools_computed_free_slots",
                stations=len(parsed_slots),
            )

        # If scheduled_sats was empty, read committed norad_ids from the DB
        # so we don't re-schedule satellites that greedy already covered.
        if not scheduled_sats:
            scheduled_sats = [
                row[0]
                for row in db.query(SatellitePass.norad_id)
                .filter(SatellitePass.is_scheduled == True)  # noqa: E712
                .distinct()
                .all()
            ]
            logger.info(
                "run_ortools_computed_scheduled_sats",
                count=len(scheduled_sats),
            )

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
# Dead-letter queue — permanently-failed task auditing
# ---------------------------------------------------------------------------

@app.task(queue="errors", ignore_result=True)
def record_dead_letter(
    task_name: str,
    task_id: str,
    error: str,
    error_type: str,
) -> None:
    """
    Receives permanently-failed tasks (MaxRetriesExceededError) for audit.

    In production wire in a real alerting channel here:
      - PagerDuty: call their Events API
      - Slack:     post to a webhook URL
      - Email:     use smtplib or an SMTP relay
    """
    logger.error(
        "dead_letter_received",
        task_name=task_name,
        task_id=task_id,
        error=error,
        error_type=error_type,
    )


@task_failure.connect
def _on_task_failure(sender, task_id, exception, traceback, einfo, **kwargs) -> None:
    """
    Route tasks to the dead-letter queue when they have exhausted all retries.

    task_failure fires on every failure (including retriable ones).
    We only route to dead-letter when MaxRetriesExceededError is raised, which
    Celery raises after the last retry attempt fails.
    """
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
            logger.error(
                "dead_letter_dispatch_failed",
                task_id=task_id,
                error=str(exc),
            )


# ---------------------------------------------------------------------------
# Queue depth monitor
# ---------------------------------------------------------------------------

@app.task(queue="ingestion")
def check_queue_depth() -> Dict[str, Any]:
    """
    Report pending task counts per queue for backpressure alerting.

    Uses Redis LLEN directly — Celery's Redis transport stores each named
    queue as a plain Redis list (LPUSH on enqueue, BRPOP on consume).

    Why NOT inspect.active_queues():
        active_queues() returns queue *topology* per worker (name, exchange,
        routing key, durable flag) — it has no 'messages' key and gives no
        information about how many tasks are waiting.  Using it with
        q.get('messages', 0) always returns 0, making monitoring a no-op.
    """
    import redis as sync_redis

    # from_url() always returns Redis at runtime; stubs type it as Redis | None.
    r: sync_redis.Redis = sync_redis.from_url(  # type: ignore[assignment]
        config.CELERY_BROKER_URL, decode_responses=True
    )
    try:
        # cast: llen() stubs return Awaitable[int] | int (shared sync/async mixin);
        # we hold a sync Redis so the return is always int at runtime.
        per_queue: Dict[str, int] = {name: cast(int, r.llen(name)) for name in _QUEUE_NAMES}
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
# Beat schedule (periodic tasks)
# ---------------------------------------------------------------------------

app.conf.beat_schedule = {
    "ingest-tles-every-6-hours": {
        "task":    "sda_system.workers.celery_app.ingest_tles",
        "schedule": crontab(minute="0", hour="*/6"),
        "options": {"queue": "ingestion"},
    },
    # Fix: check_queue_depth existed but was never scheduled — monitoring never ran
    "check-queue-depth-every-5-minutes": {
        "task":    "sda_system.workers.celery_app.check_queue_depth",
        "schedule": crontab(minute="*/5"),
        "options": {"queue": "ingestion"},
    },
}

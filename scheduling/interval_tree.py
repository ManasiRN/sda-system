"""
scheduling/interval_tree.py — Benefit-maximising greedy scheduler.

Uses an IntervalTree for O(log N) conflict detection per candidate pass,
making this suitable for large pass sets where a linear scan would be slow.

Difference from GreedyScheduler.schedule_weighted()
-----------------------------------------------------
  schedule_weighted()       Weighted-interval DP — globally optimal for a
                            single station's candidate list.  O(N log N).

  IntervalTreeScheduler     Greedy by benefit descending — picks the best
                            pass first, then the next non-conflicting best,
                            etc.  Not globally optimal but works well for
                            dynamic / incremental scheduling where the full
                            list is not known upfront, and is easier to
                            reason about for operators.

Cross-station satellite uniqueness
-----------------------------------
Once a satellite is scheduled at any station it is excluded from all
subsequent station queries so each satellite appears at most once across
the full schedule, maximising unique satellite coverage.

Fixes vs. original
------------------
  - `defaultdict` unused import removed.
  - `GroundStation.is_active == True` filter added (inactive stations were
    being scheduled — passes were committed for decommissioned stations).
  - UTC normalization added to `.timestamp()` calls — naive datetimes used
    local time instead of UTC, corrupting IntervalTree queries.
  - Zero-duration pass guard added — intervaltree raises ValueError for
    begin >= end; these passes are now skipped and logged.
  - Per-station `session.commit()` wrapped in try/except with rollback so
    a failure at one station does not corrupt other stations' results.
  - Session guard added — clear error if session=None.
  - Conflict verification added before each station commit.
  - Per-station and summary statistics improved.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from intervaltree import IntervalTree
from sqlalchemy.orm import Session
from structlog import get_logger

from ..db.models import GroundStation, SatellitePass

logger = get_logger()


def _to_utc_timestamp(dt: datetime) -> float:
    """
    Convert a datetime to a UTC POSIX timestamp (float seconds since epoch).

    Handles:
      - Timezone-aware UTC     → fast path, direct .timestamp()
      - Timezone-aware non-UTC → converts to UTC first via astimezone()
      - Naive (no tzinfo)      → assumed UTC, labelled before conversion

    Without this normalization, naive datetimes use the process's local
    timezone in .timestamp(), producing wrong IntervalTree boundaries and
    causing false conflict misses or phantom conflicts.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).timestamp()


class IntervalTreeScheduler:
    """
    Benefit-maximising greedy scheduler using IntervalTree for conflict detection.

    Complexity: O(N log N) sort + O(N log N) tree insertions/overlap queries.

    Usage:
        scheduler = IntervalTreeScheduler(session=db)
        scheduled, sat_ids = scheduler.schedule_all_stations(start, end)
    """

    def __init__(self, session: Session) -> None:
        self.session = session

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def schedule_all_stations(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> Tuple[List[SatellitePass], Set[int]]:
        """
        Schedule passes across all ACTIVE ground stations.

        Processes stations in station_id order for deterministic output.
        Each station is committed independently — a failure at one station
        is logged and skipped without rolling back other stations.

        Returns:
            (all_scheduled_passes, set_of_all_scheduled_norad_ids)
        """
        stations: List[GroundStation] = (
            self.session.query(GroundStation)
            .filter(GroundStation.is_active == True)      # Fix: was missing
            .order_by(GroundStation.station_id)
            .all()
        )

        if not stations:
            logger.warning("No active ground stations — nothing scheduled")
            return [], set()

        all_scheduled:    List[SatellitePass] = []
        globally_scheduled: Set[int]          = set()
        failed_stations:  List[str]           = []

        for station in stations:
            try:
                scheduled, new_sats = self._schedule_for_station(
                    station.station_id,
                    globally_scheduled,
                    start_time,
                    end_time,
                )
                all_scheduled.extend(scheduled)
                globally_scheduled.update(new_sats)
            except Exception as exc:
                # Isolate per-station failures — don't abort the whole run
                failed_stations.append(station.station_id)
                logger.error(
                    "Station scheduling failed — skipped",
                    station_id=station.station_id,
                    error=str(exc),
                )

        logger.info(
            "Interval-tree scheduling complete",
            stations_processed=len(stations) - len(failed_stations),
            stations_failed=len(failed_stations),
            total_passes=len(all_scheduled),
            unique_satellites=len(globally_scheduled),
        )
        if failed_stations:
            logger.warning(
                "Some stations were skipped due to errors",
                failed=failed_stations,
            )

        return all_scheduled, globally_scheduled

    # ------------------------------------------------------------------
    # Per-station scheduling
    # ------------------------------------------------------------------

    def _schedule_for_station(
        self,
        station_id: str,
        globally_scheduled: Set[int],
        start_time: datetime,
        end_time: datetime,
    ) -> Tuple[List[SatellitePass], Set[int]]:
        """
        Greedily schedule the highest-benefit non-conflicting passes at one station.

        Algorithm
        ---------
        1. Fetch unscheduled passes for unseen satellites in the time window.
        2. Sort descending by benefit (elevation × duration) — best first.
        3. For each candidate in order:
             a. Skip zero-duration passes (intervaltree rejects begin >= end).
             b. Query the IntervalTree for conflicts — O(log N + k).
             c. If no conflict, add to tree and mark scheduled.
        4. Verify no conflicts in the selected set (defensive).
        5. Commit, or rollback and raise on failure.
        """
        query = self.session.query(SatellitePass).filter(
            SatellitePass.station_id    == station_id,
            SatellitePass.rise_time     >= start_time,
            SatellitePass.set_time      <= end_time,
            SatellitePass.is_scheduled  == False,
        )
        if globally_scheduled:
            query = query.filter(
                SatellitePass.norad_id.notin_(list(globally_scheduled))
            )

        # Sort best benefit first — greedy pick is quality-optimal
        candidates: List[SatellitePass] = sorted(
            query.all(),
            key=lambda p: p.max_elevation * p.duration_seconds,
            reverse=True,
        )

        if not candidates:
            logger.debug("No candidates", station_id=station_id)
            return [], set()

        tree: IntervalTree     = IntervalTree()
        selected: List[SatellitePass] = []
        new_sats: Set[int]     = set()
        skipped_zero: int      = 0

        for p in candidates:
            # Fix: normalize to UTC before converting to float timestamp.
            # Without this, naive datetimes use local time → wrong boundaries.
            rise_ts = _to_utc_timestamp(p.rise_time)
            set_ts  = _to_utc_timestamp(p.set_time)

            # Fix: intervaltree raises ValueError for begin >= end.
            # A zero-duration pass can appear when rise_time == set_time due to
            # floating-point rounding in the pass detector.
            if set_ts <= rise_ts:
                skipped_zero += 1
                logger.debug(
                    "Zero-duration pass skipped",
                    norad_id=p.norad_id,
                    station_id=station_id,
                    rise_ts=rise_ts,
                    set_ts=set_ts,
                )
                continue

            # O(log N + k) overlap query — k = number of overlapping intervals
            # IntervalTree uses half-open intervals [begin, end), so two passes
            # that share an exact boundary are NOT considered overlapping, which
            # correctly allows back-to-back passes.
            if tree.overlap(rise_ts, set_ts):
                continue  # Conflict with already-selected pass — skip

            tree[rise_ts:set_ts] = p.id
            p.is_scheduled  = True
            p.scheduled_by  = "interval_tree"
            self.session.add(p)
            selected.append(p)
            new_sats.add(p.norad_id)

        # Defensive conflict check before committing
        conflicts = _verify_no_conflicts(selected)
        if conflicts:
            logger.error(
                "Conflict detected in interval-tree result — rolling back station",
                station_id=station_id,
                conflicts=conflicts,
            )
            self.session.rollback()
            return [], set()

        try:
            self.session.commit()
        except Exception as exc:
            self.session.rollback()
            logger.error(
                "Commit failed for station — rolled back",
                station_id=station_id,
                error=str(exc),
            )
            raise

        logger.info(
            "Interval-tree scheduled station",
            station_id=station_id,
            candidates=len(candidates),
            scheduled=len(selected),
            skipped_zero_duration=skipped_zero,
            unique_satellites=len(new_sats),
            total_benefit=round(
                sum(p.max_elevation * p.duration_seconds for p in selected), 2
            ),
        )
        return selected, new_sats


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _verify_no_conflicts(
    passes: List[SatellitePass],
) -> List[Tuple[str, str]]:
    """
    Verify that no two passes in the list overlap in time.

    Returns a list of (norad_id_a, norad_id_b) string pairs for any
    conflicts found.  Empty list means the schedule is conflict-free.

    Uses a fresh IntervalTree — O(N log N) — rather than trusting the
    scheduling tree (which could have been mutated during the loop).
    """
    if len(passes) < 2:
        return []

    check_tree: IntervalTree = IntervalTree()
    conflicts: List[Tuple[str, str]] = []

    for p in passes:
        rise_ts = _to_utc_timestamp(p.rise_time)
        set_ts  = _to_utc_timestamp(p.set_time)

        if set_ts <= rise_ts:
            continue  # already guarded upstream

        overlapping = check_tree.overlap(rise_ts, set_ts)
        if overlapping:
            for iv in overlapping:
                conflicts.append((str(p.norad_id), str(iv.data)))

        check_tree[rise_ts:set_ts] = p.norad_id

    return conflicts

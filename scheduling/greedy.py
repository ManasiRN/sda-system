"""
scheduling/greedy.py — Production-grade satellite pass scheduler.

Three algorithm variants, each optimal for a different objective:

  schedule_edf()       Earliest-Deadline-First greedy.
                       Maximises the COUNT of passes scheduled.
                       O(N log N) sort + O(N) scan.

  schedule_weighted()  Weighted-interval-scheduling DP with binary search.
                       Maximises total observation quality
                       (elevation × duration benefit).
                       O(N log N) — prev[] built with bisect, NOT a nested
                       loop (old code was silently O(N²)).

  schedule_heap()      Max-heap greedy ordered by benefit.
                       Best for dynamic / incremental scheduling where the
                       full pass list is not known upfront.
                       O(N log N) with O(log N) overlap check per candidate.

DB-backed wrappers:
  schedule_all_stations()  Weighted scheduling across all active stations
                           with cross-station uniqueness and conflict guard.
  get_free_slots()         Returns unoccupied time windows at a station,
                           merging overlapping occupied intervals first.

Fixes vs. original
------------------
  - datetime.min (naive) replaced by timezone-aware _EPOCH_MIN sentinel →
    eliminates TypeError when comparing with tz-aware DB timestamps.
  - prev[] computation changed from O(N²) nested loop to bisect_right →
    O(N log N) for the full weighted DP.
  - schedule_heap() added: heap + sorted interval list + bisect overlap check.
  - _detect_conflicts() added: O(N log N) sweep-line post-scheduling guard.
  - _merge_intervals() added: correctly merges overlapping scheduled windows
    before computing free slots (old code could emit phantom free gaps).
  - session=None guard prevents AttributeError on unintialised scheduler.
  - Inactive stations are explicitly filtered in schedule_all_stations().
"""

import bisect
import heapq
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional, Set, Tuple

from sqlalchemy.orm import Session
from structlog import get_logger

from ..db.models import GroundStation, SatellitePass

logger = get_logger()

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

# Timezone-aware sentinel — guaranteed to be before any real satellite pass.
# Replaces the old `datetime.min` (naive) which raised TypeError on comparison
# with timezone-aware DB timestamps.
_EPOCH_MIN: datetime = datetime(1970, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Benefit function — single source of truth
# ---------------------------------------------------------------------------

def _benefit(p: SatellitePass) -> float:
    """Observation quality score used by all three schedulers."""
    return p.max_elevation * p.duration_seconds


# ---------------------------------------------------------------------------
# GreedyScheduler
# ---------------------------------------------------------------------------

class GreedyScheduler:
    """
    Satellite pass scheduler — three algorithm variants plus DB wrappers.

    Instantiate with a SQLAlchemy Session for DB-backed methods:
        scheduler = GreedyScheduler(session=db)
        scheduled, sat_ids = scheduler.schedule_all_stations(start, end)

    Static methods need no session:
        selected = GreedyScheduler.schedule_weighted(candidate_passes)
    """

    def __init__(self, session: Optional[Session] = None) -> None:
        self.session = session

    def _require_session(self) -> Session:
        if self.session is None:
            raise RuntimeError(
                "This method requires a database session. "
                "Pass session=db when constructing GreedyScheduler."
            )
        return self.session

    # ------------------------------------------------------------------
    # DB-backed public methods
    # ------------------------------------------------------------------

    def schedule_all_stations(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> Tuple[List[SatellitePass], Set[int]]:
        """
        Schedule passes across all active ground stations using weighted DP.

        Cross-station uniqueness: once a satellite is scheduled at any station
        it is excluded from all subsequent station queries so each satellite
        appears in the schedule exactly once, maximising unique coverage.

        After scheduling, _detect_conflicts() verifies the result before the
        DB commit — any conflicting passes are dropped and logged.

        Returns:
            (all_scheduled_passes, set_of_scheduled_norad_ids)
        """
        db = self._require_session()

        stations: List[GroundStation] = (
            db.query(GroundStation)
            .filter(GroundStation.is_active == True)
            .order_by(GroundStation.station_id)
            .all()
        )

        if not stations:
            logger.warning("No active ground stations found — nothing scheduled")
            return [], set()

        all_scheduled: List[SatellitePass] = []
        globally_scheduled_sats: Set[int] = set()
        total_candidates = 0
        total_conflicts = 0

        for station in stations:
            query = db.query(SatellitePass).filter(
                SatellitePass.station_id == station.station_id,
                SatellitePass.rise_time  >= start_time,
                SatellitePass.set_time   <= end_time,
                SatellitePass.is_scheduled == False,
            )
            # Exclude satellites already scheduled at another station
            if globally_scheduled_sats:
                query = query.filter(
                    SatellitePass.norad_id.notin_(list(globally_scheduled_sats))
                )

            candidates: List[SatellitePass] = query.all()
            total_candidates += len(candidates)

            if not candidates:
                logger.debug("No candidates", station_id=station.station_id)
                continue

            selected = self.schedule_weighted(candidates)

            # --- Defensive conflict check before committing ---
            conflicts = _detect_conflicts(selected)
            if conflicts:
                total_conflicts += len(conflicts)
                bad_ids: Set[int] = {
                    id(p) for pair in conflicts for p in pair
                }
                before = len(selected)
                selected = [p for p in selected if id(p) not in bad_ids]
                logger.error(
                    "Schedule conflicts detected — conflicting passes removed",
                    station_id=station.station_id,
                    conflicts=len(conflicts),
                    removed=before - len(selected),
                )

            for p in selected:
                p.is_scheduled  = True
                p.scheduled_by  = "greedy"
                db.add(p)
                globally_scheduled_sats.add(p.norad_id)
                all_scheduled.append(p)

        db.commit()

        logger.info(
            "Greedy scheduling complete",
            stations=len(stations),
            candidates=total_candidates,
            scheduled=len(all_scheduled),
            unique_satellites=len(globally_scheduled_sats),
            conflicts_removed=total_conflicts,
        )
        return all_scheduled, globally_scheduled_sats

    def get_free_slots(
        self,
        station_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> List[Tuple[str, str]]:
        """
        Return unoccupied time windows at a station as ISO-format string pairs.

        Merges overlapping/adjacent scheduled intervals before computing gaps.
        The old code could emit a phantom free slot between two passes where
        one extended past the end of the previous without this merge step.

        Returns serialisable (ISO str, ISO str) pairs for Celery task args.
        """
        db = self._require_session()

        scheduled: List[SatellitePass] = (
            db.query(SatellitePass)
            .filter(
                SatellitePass.station_id  == station_id,
                SatellitePass.is_scheduled == True,
                SatellitePass.rise_time   >= start_time,
                SatellitePass.set_time    <= end_time,
            )
            .order_by(SatellitePass.rise_time)
            .all()
        )

        # Merge overlapping occupied windows before gap computation
        occupied = _merge_intervals(
            [(p.rise_time, p.set_time) for p in scheduled]
        )

        free_slots: List[Tuple[str, str]] = []
        cursor = start_time

        for occ_start, occ_end in occupied:
            if occ_start > cursor:
                free_slots.append((cursor.isoformat(), occ_start.isoformat()))
            cursor = max(cursor, occ_end)

        if cursor < end_time:
            free_slots.append((cursor.isoformat(), end_time.isoformat()))

        return free_slots

    # ------------------------------------------------------------------
    # Pure-algorithm static methods — no DB, no side effects
    # ------------------------------------------------------------------

    @staticmethod
    def schedule_edf(passes: List[SatellitePass]) -> List[SatellitePass]:
        """
        Earliest-Deadline-First interval scheduling.

        Selects passes in set_time order, greedily accepting each one that
        does not overlap the last accepted pass.  Optimal for maximising the
        total COUNT of passes scheduled.

        Complexity: O(N log N) sort + O(N) linear scan.

        Fix: replaced `datetime.min` (naive, caused TypeError) with
        `_EPOCH_MIN`, a timezone-aware sentinel before all real pass times.
        """
        if not passes:
            return []

        sorted_passes = sorted(passes, key=lambda p: p.set_time)
        scheduled: List[SatellitePass] = []
        last_end: datetime = _EPOCH_MIN  # tz-aware — never raises TypeError

        for p in sorted_passes:
            if p.rise_time >= last_end:
                scheduled.append(p)
                last_end = p.set_time

        return scheduled

    @staticmethod
    def schedule_weighted(passes: List[SatellitePass]) -> List[SatellitePass]:
        """
        Weighted-interval scheduling via DP with O(log N) binary search.

        Benefit function: max_elevation × duration_seconds.
        Maximises total observation quality while resolving time conflicts.

        Fix: the original O(N²) nested loop for prev[] is replaced by
        bisect.bisect_right on the sorted set_time array — O(log N) per pass,
        making the full algorithm O(N log N).

        DP definition (1-indexed):
          dp[i] = max benefit achievable using only passes 1..i
          dp[0] = 0 (base case)
          dp[i] = max(
            benefits[i-1] + dp[prev[i-1] + 1],   # include pass i
            dp[i-1]                                # skip pass i
          )
        where prev[i] = index of the last pass compatible with pass i
        (set_time ≤ rise_time[i]), computed via bisect_right.
        """
        if not passes:
            return []

        sorted_passes = sorted(passes, key=lambda p: p.set_time)
        n = len(sorted_passes)

        benefits: List[float] = [_benefit(p) for p in sorted_passes]

        # Extract set_times into a plain list so bisect can search it in O(log N)
        set_times: List[datetime] = [p.set_time for p in sorted_passes]

        # prev[i]: 0-based index of the latest pass whose set_time ≤ rise_time[i].
        # -1 means no compatible predecessor exists.
        # bisect_right(set_times, rise_time) gives the insertion point such that
        # all elements to the left are ≤ rise_time → subtract 1 for last index.
        prev: List[int] = [
            bisect.bisect_right(set_times, sorted_passes[i].rise_time) - 1
            for i in range(n)
        ]

        # Forward DP (1-indexed to keep dp[0] = 0 as a clean base case)
        dp: List[float] = [0.0] * (n + 1)
        for i in range(1, n + 1):
            include = benefits[i - 1] + dp[prev[i - 1] + 1]
            exclude = dp[i - 1]
            dp[i] = max(include, exclude)

        # Backward reconstruction: collect passes that contributed to dp[n]
        selected: List[SatellitePass] = []
        i = n
        while i > 0:
            if dp[i] != dp[i - 1]:
                # Pass i-1 was included
                selected.append(sorted_passes[i - 1])
                i = prev[i - 1] + 1
            else:
                i -= 1

        return selected

    @staticmethod
    def schedule_heap(passes: List[SatellitePass]) -> List[SatellitePass]:
        """
        Max-heap greedy scheduler ordered by benefit (elevation × duration).

        Pops the highest-benefit candidate from the heap, schedules it if it
        does not conflict with any already-selected pass (checked in O(log N)
        via bisect on a sorted interval list), then discards conflicting passes
        lazily as they are popped.

        When to prefer this over schedule_weighted():
          - Streaming / online scheduling: passes arrive incrementally.
          - Very large candidate sets (> 100 k) where DP memory is a concern.
          - A fast approximate solution is acceptable (not the exact DP optimum).

        Complexity: O(N log N) heapify + O(N log N) total conflict checks.

        Implementation notes
        --------------------
        Heap tuple: (-benefit, unique_index, pass_object)
          - Negated benefit turns Python's min-heap into a max-heap.
          - unique_index prevents Python from ever comparing SatellitePass
            objects when two benefits are equal (SatellitePass has no __lt__).
        Conflict check: bisect against a sorted list of (rise_time, set_time)
          pairs — only the immediate neighbours need to be inspected.
        """
        if not passes:
            return []

        # Build max-heap (negated benefit so heapq.heappop gives the best first)
        heap: List[Tuple[float, int, SatellitePass]] = [
            (-_benefit(p), idx, p) for idx, p in enumerate(passes)
        ]
        heapq.heapify(heap)

        # Sorted list of committed (rise_time, set_time) pairs for bisect search
        committed: List[Tuple[datetime, datetime]] = []
        scheduled: List[SatellitePass] = []

        while heap:
            _, _, p = heapq.heappop(heap)

            if _has_overlap(p.rise_time, p.set_time, committed):
                continue  # Conflict with an already-scheduled pass — skip

            scheduled.append(p)
            # Keep committed sorted by rise_time for O(log N) future checks
            bisect.insort(committed, (p.rise_time, p.set_time))

        return scheduled

    # ------------------------------------------------------------------
    # Convenience: iterate scheduled passes in time order
    # ------------------------------------------------------------------

    @staticmethod
    def timeline(
        passes: List[SatellitePass],
    ) -> Iterator[SatellitePass]:
        """Yield scheduled passes sorted by rise_time (ascending)."""
        yield from sorted(passes, key=lambda p: p.rise_time)


# ---------------------------------------------------------------------------
# Private helpers — pure functions, no DB access
# ---------------------------------------------------------------------------

def _has_overlap(
    rise: datetime,
    set_: datetime,
    sorted_intervals: List[Tuple[datetime, datetime]],
) -> bool:
    """
    O(log N) overlap check against a sorted list of (rise, set) pairs.

    Uses bisect to find the insertion point for (rise, set_), then inspects
    only the two immediate neighbours.  This is sufficient because:
      - Any interval to the LEFT with set_time > rise would conflict.
      - Any interval to the RIGHT with rise_time < set_ would conflict.

    Two intervals [a, b) and [c, d) overlap iff  a < d  and  c < b.
    """
    if not sorted_intervals:
        return False

    pos = bisect.bisect_left(sorted_intervals, (rise, set_))

    # Check left neighbour
    if pos > 0:
        prev_rise, prev_set = sorted_intervals[pos - 1]
        if prev_rise < set_ and rise < prev_set:
            return True

    # Check right neighbour
    if pos < len(sorted_intervals):
        next_rise, next_set = sorted_intervals[pos]
        if rise < next_set and next_rise < set_:
            return True

    return False


def _detect_conflicts(
    passes: List[SatellitePass],
) -> List[Tuple[SatellitePass, SatellitePass]]:
    """
    Sweep-line conflict detector — O(N log N).

    Returns all overlapping (pass_a, pass_b) pairs in the input list.
    An empty return means the schedule is conflict-free.

    Used as a defensive post-check in schedule_all_stations() before
    writing to the database.  Two passes conflict when:
        pass_a.rise_time < pass_b.set_time  AND
        pass_b.rise_time < pass_a.set_time

    Algorithm
    ---------
    Sort by rise_time.  Maintain a min-heap of active passes keyed on
    set_time so that expired passes (set_time ≤ current rise_time) can be
    purged in O(log N).  Any pass remaining in the heap when a new pass
    arrives must overlap with it.
    """
    if len(passes) < 2:
        return []

    conflicts: List[Tuple[SatellitePass, SatellitePass]] = []
    sorted_passes = sorted(passes, key=lambda p: p.rise_time)

    # Heap of (set_time, unique_idx, pass) — unique_idx prevents comparison
    # of SatellitePass objects when two set_times are equal.
    active: List[Tuple[datetime, int, SatellitePass]] = []

    for idx, p in enumerate(sorted_passes):
        # Expire passes that ended at or before p.rise_time
        while active and active[0][0] <= p.rise_time:
            heapq.heappop(active)

        # Every remaining active pass overlaps with p
        for _, _, active_pass in active:
            conflicts.append((active_pass, p))

        heapq.heappush(active, (p.set_time, idx, p))

    return conflicts


def _merge_intervals(
    intervals: List[Tuple[datetime, datetime]],
) -> List[Tuple[datetime, datetime]]:
    """
    Merge overlapping or adjacent (start, end) pairs — O(N log N).

    Used by get_free_slots() to collapse back-to-back or overlapping
    scheduled passes into single occupied windows before gap computation.
    Without this merge, a pass extending past the end of the previous one
    would produce a phantom free slot between them.
    """
    if not intervals:
        return []

    sorted_ivs = sorted(intervals, key=lambda iv: iv[0])
    merged: List[Tuple[datetime, datetime]] = [sorted_ivs[0]]

    for start, end in sorted_ivs[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end:
            # Overlapping or adjacent — extend the current window
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))

    return merged

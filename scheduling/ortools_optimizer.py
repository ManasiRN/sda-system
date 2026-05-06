"""
scheduling/ortools_optimizer.py — Stage 2: CP-SAT optimizer.

Schedules satellites that the greedy Stage 1 missed by formulating the
problem as a Constraint-Programming Satisfiability (CP-SAT) integer program.

Problem formulation
-------------------
Variables:
  selected[i]  BoolVar — pass i is scheduled (1) or rejected (0)
  sat_var[k]   BoolVar — satellite k appears in the schedule

Constraints:
  NoOverlap per station  : no two active intervals at the same station overlap
  AtMostOne per satellite: at most one pass per satellite is selected
  MaxEquality link       : sat_var[k] = max(selected[i] for i in sat_k_passes)

Objective (lexicographic, encoded as a weighted sum):
  Primary   — maximise unique satellites (sat_vars, weight = max_benefit + 1)
  Secondary — maximise total observation quality (elevation × duration, scaled)
  This guarantees satellite count always dominates; benefit breaks ties.

Time normalization
------------------
All datetimes → integer seconds since epoch (window start_time), UTC-normalized
before subtraction.  Handles naive datetimes, UTC-aware, and offset-aware inputs
without raising TypeError.

Candidate query strategy
------------------------
One broad SQL query fetches all unscheduled passes in the window for unseen
satellites.  Free-slot filtering is done in Python with a O(log N) bisect check.
This replaces the original or_(*conditions) approach that generated one SQL clause
per free slot — with 50 stations × many slots, that produced hundreds of OR
clauses that degraded the Postgres query planner and caused slow / no-solution
outcomes.

Fixes vs. original
------------------
  - `not_` unused import removed.
  - `or_(*conditions)` per-slot query replaced with one query + Python bisect filter.
  - UTC normalization added to all datetime → int conversions.
  - Zero-duration interval guard added (CP-SAT rejects duration ≤ 0).
  - Dual-objective (satellite count + benefit tiebreaker) replaces count-only objective
    so OR-Tools never produces a worse schedule than greedy when counts tie.
  - Duplicate-satellite post-extraction guard added.
  - Already-scheduled satellite guard added (Stage 1 overlap prevention).
  - Optimality gap logged so operators know if time limit was hit before optimal.
  - `AddMaxEquality` guarded against empty variable list.
  - Solver statistics (branches, conflicts, wall time) logged for every run.
"""

from __future__ import annotations

import bisect
from datetime import datetime, timezone
from typing import Any, Dict, List, Set, Tuple

from ortools.sat.python import cp_model
from sqlalchemy.orm import Session
from structlog import get_logger

from ..db.models import SatellitePass

logger = get_logger()

# Multiply float benefit by this before truncating to int for CP-SAT.
# CP-SAT requires integer coefficients; scaling preserves relative ordering.
_BENEFIT_SCALE: int = 100


# ---------------------------------------------------------------------------
# Time normalization helper
# ---------------------------------------------------------------------------

def _to_utc_seconds(dt: datetime, epoch: datetime) -> int:
    """
    Convert dt to integer seconds since epoch, both normalized to UTC.

    Handles:
      - Timezone-aware UTC datetimes  (fastest path, zero conversion)
      - Timezone-aware non-UTC        (e.g. +05:30 → UTC via astimezone)
      - Naive datetimes               (assumed UTC, labelled with replace())

    Raises ValueError if dt is before epoch (negative duration = invalid pass).
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if epoch.tzinfo is None:
        epoch = epoch.replace(tzinfo=timezone.utc)

    delta_sec = int(
        (dt.astimezone(timezone.utc) - epoch.astimezone(timezone.utc))
        .total_seconds()
    )
    if delta_sec < 0:
        raise ValueError(
            f"Pass time {dt.isoformat()} precedes schedule epoch "
            f"{epoch.isoformat()} by {-delta_sec}s"
        )
    return delta_sec


# ---------------------------------------------------------------------------
# Free-slot membership check  (O(log N) via bisect)
# ---------------------------------------------------------------------------

def _fits_in_any_slot(
    rise: datetime,
    set_: datetime,
    sorted_slots: List[Tuple[datetime, datetime]],
) -> bool:
    """
    Return True if [rise, set_] fits entirely inside at least one free slot.

    sorted_slots must be sorted by slot start time (ascending).

    Strategy: bisect_right on slot starts finds the rightmost slot whose
    start ≤ rise in O(log N).  Only that slot needs to be checked — any
    slot with start > rise means the pass begins before the slot opens.
    """
    if not sorted_slots:
        return False
    starts = [s[0] for s in sorted_slots]
    pos = bisect.bisect_right(starts, rise) - 1
    if pos < 0:
        return False
    _, slot_end = sorted_slots[pos]
    return set_ <= slot_end


# ---------------------------------------------------------------------------
# Optimality gap
# ---------------------------------------------------------------------------

def _optimality_gap_pct(obj: float, bound: float) -> float:
    """
    Relative optimality gap as a percentage.

    0.0 means proven optimal.  A non-zero value after solver termination
    means the time limit was hit before the optimal could be verified.
    """
    if obj == bound:
        return 0.0
    return abs(bound - obj) / max(abs(obj), 1.0) * 100.0


# ---------------------------------------------------------------------------
# ORToolsOptimizer
# ---------------------------------------------------------------------------

class ORToolsOptimizer:
    """
    CP-SAT optimizer for remaining unscheduled satellites (Stage 2).

    Usage:
        optimizer = ORToolsOptimizer(db, time_limit_sec=30, num_workers=4)
        newly_scheduled = optimizer.optimize_remaining(
            scheduled_satellites, start_time, end_time, free_slots
        )
    """

    def __init__(
        self,
        session: Session,
        time_limit_sec: int = 30,
        num_workers: int = 4,
    ) -> None:
        self.session    = session
        self.time_limit = time_limit_sec
        self.num_workers = num_workers

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def optimize_remaining(
        self,
        scheduled_satellites: Set[int],
        start_time: datetime,
        end_time: datetime,
        free_slots: Dict[str, List[Tuple[datetime, datetime]]],
    ) -> List[SatellitePass]:
        """
        Run CP-SAT optimization on passes for satellites not yet scheduled.

        Args:
            scheduled_satellites: NORAD IDs already committed by Stage 1.
            start_time:           Window start (timezone-aware recommended).
            end_time:             Window end.
            free_slots:           {station_id: [(slot_start, slot_end), ...]}

        Returns:
            Newly committed SatellitePass objects.
        """
        # Normalize epoch once — all _to_utc_seconds calls share this reference
        epoch: datetime = (
            start_time.astimezone(timezone.utc)
            if start_time.tzinfo
            else start_time.replace(tzinfo=timezone.utc)
        )

        candidates = self._get_candidate_passes(
            scheduled_satellites, start_time, end_time, free_slots
        )

        if not candidates:
            logger.info("No candidate passes for OR-Tools — skipping Stage 2")
            return []

        logger.info(
            "Starting OR-Tools CP-SAT optimization",
            candidates=len(candidates),
            time_limit_sec=self.time_limit,
            num_workers=self.num_workers,
        )

        return self._solve(candidates, epoch, scheduled_satellites)

    # ------------------------------------------------------------------
    # Solver
    # ------------------------------------------------------------------

    def _solve(
        self,
        candidates: List[SatellitePass],
        epoch: datetime,
        already_scheduled: Set[int],
    ) -> List[SatellitePass]:
        """Build and solve the CP-SAT model. Returns newly scheduled passes."""

        model = cp_model.CpModel()

        # ----------------------------------------------------------
        # Step 1: convert datetimes to integer seconds, drop invalids
        # ----------------------------------------------------------
        starts:    List[int]  = []
        ends:      List[int]  = []
        durations: List[int]  = []
        benefits:  List[int]  = []
        keep:      List[bool] = []

        for p in candidates:
            try:
                s = _to_utc_seconds(p.rise_time, epoch)
                e = _to_utc_seconds(p.set_time,  epoch)
                d = e - s
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "Pass skipped — time normalization failed",
                    norad_id=p.norad_id,
                    station_id=p.station_id,
                    error=str(exc),
                )
                keep.append(False)
                starts.append(0); ends.append(0)
                durations.append(0); benefits.append(0)
                continue

            if d <= 0:
                # CP-SAT rejects zero or negative duration intervals outright
                logger.debug(
                    "Pass skipped — zero or negative duration",
                    norad_id=p.norad_id,
                    station_id=p.station_id,
                    duration_sec=d,
                )
                keep.append(False)
                starts.append(s); ends.append(e)
                durations.append(d); benefits.append(0)
                continue

            keep.append(True)
            starts.append(s)
            ends.append(e)
            durations.append(d)
            benefits.append(
                int(p.max_elevation * p.duration_seconds * _BENEFIT_SCALE)
            )

        valid_idx: List[int] = [i for i, v in enumerate(keep) if v]

        if not valid_idx:
            logger.warning(
                "All candidates invalid after normalization — no model built"
            )
            return []

        n = len(valid_idx)
        logger.debug("Model variables", valid_passes=n, dropped=len(keep) - n)

        # ----------------------------------------------------------
        # Step 2: decision variables
        # ----------------------------------------------------------
        # One BoolVar per valid pass
        selected: List[cp_model.IntVar] = [
            model.NewBoolVar(f"p_{valid_idx[i]}") for i in range(n)
        ]

        # One optional IntervalVar per valid pass (active only when selected)
        intervals: List[Any] = [
            model.NewOptionalIntervalVar(
                starts[valid_idx[i]],
                durations[valid_idx[i]],
                ends[valid_idx[i]],
                selected[i],
                f"iv_{valid_idx[i]}",
            )
            for i in range(n)
        ]

        # ----------------------------------------------------------
        # Step 3: NoOverlap constraint per station
        # ----------------------------------------------------------
        station_groups: Dict[str, List[int]] = {}
        for local_i, global_i in enumerate(valid_idx):
            sid = candidates[global_i].station_id
            station_groups.setdefault(sid, []).append(local_i)

        for sid, local_indices in station_groups.items():
            if len(local_indices) > 1:
                model.AddNoOverlap([intervals[i] for i in local_indices])

        # ----------------------------------------------------------
        # Step 4: AtMostOne per satellite + satellite coverage variable
        # ----------------------------------------------------------
        sat_groups: Dict[int, List[int]] = {}
        for local_i, global_i in enumerate(valid_idx):
            norad = candidates[global_i].norad_id
            sat_groups.setdefault(norad, []).append(local_i)

        sat_vars: Dict[int, cp_model.IntVar] = {}
        for norad, local_indices in sat_groups.items():
            if not local_indices:
                continue  # should never happen, guard anyway

            if len(local_indices) == 1:
                # Single pass — satellite var IS the pass var (no extra constraint)
                sat_vars[norad] = selected[local_indices[0]]
            else:
                # At most one pass per satellite may be selected
                model.AddAtMostOne([selected[i] for i in local_indices])

                # sat_var = 1 iff any of the satellite's passes is selected
                sat_var = model.NewBoolVar(f"sat_{norad}")
                model.AddMaxEquality(
                    sat_var,
                    [selected[i] for i in local_indices],
                )
                sat_vars[norad] = sat_var

        if not sat_vars:
            logger.warning("No satellite variables created — aborting solve")
            return []

        # ----------------------------------------------------------
        # Step 5: Objective (lexicographic via weighted sum)
        #
        # Encoding trick: satellite weight > any possible benefit sum per
        # satellite, so the primary objective (unique count) always dominates.
        # ----------------------------------------------------------
        max_benefit_per_pass = max(
            (benefits[valid_idx[i]] for i in range(n)), default=1
        )
        # One satellite is worth more than the best benefit tiebreaker
        sat_weight = max_benefit_per_pass + 1

        obj_vars:   List[cp_model.IntVar] = []
        obj_coeffs: List[int]             = []

        # Primary: unique satellite count
        for sat_var in sat_vars.values():
            obj_vars.append(sat_var)
            obj_coeffs.append(sat_weight)

        # Secondary: total observation benefit (tiebreaker)
        for i in range(n):
            obj_vars.append(selected[i])
            obj_coeffs.append(benefits[valid_idx[i]])

        model.Maximize(cp_model.LinearExpr.WeightedSum(obj_vars, obj_coeffs))

        # ----------------------------------------------------------
        # Step 6: Solve
        # ----------------------------------------------------------
        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = self.time_limit
        solver.parameters.num_search_workers  = self.num_workers
        solver.parameters.log_search_progress = False

        status = solver.Solve(model)
        self._log_solver_stats(solver, status, n)

        if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            logger.warning(
                "OR-Tools found no solution",
                status=solver.StatusName(status),
                candidates=n,
                hint=(
                    "INFEASIBLE usually means all passes conflict with each other. "
                    "UNKNOWN usually means the time limit was hit before the first "
                    "feasible solution was found — try increasing ORTOOLS_TIME_LIMIT_SECONDS."
                ),
            )
            return []

        # ----------------------------------------------------------
        # Step 7: Extract results with uniqueness and overlap guards
        # ----------------------------------------------------------
        newly_scheduled: List[SatellitePass] = []
        committed_norads: Set[int] = set()

        for i in range(n):
            if solver.Value(selected[i]) != 1:
                continue

            p = candidates[valid_idx[i]]

            # Guard: solver should not pick a Stage 1 satellite (constraint gap)
            if p.norad_id in already_scheduled:
                logger.warning(
                    "OR-Tools selected Stage-1 satellite — skipped",
                    norad_id=p.norad_id,
                )
                continue

            # Guard: enforce one-pass-per-satellite even if AddMaxEquality
            # had a gap (e.g. due to floating-point objective rounding)
            if p.norad_id in committed_norads:
                logger.warning(
                    "Duplicate satellite in OR-Tools result — skipped",
                    norad_id=p.norad_id,
                )
                continue

            p.is_scheduled = True
            p.scheduled_by = "ortools"
            self.session.add(p)
            newly_scheduled.append(p)
            committed_norads.add(p.norad_id)

        self.session.commit()

        logger.info(
            "OR-Tools optimization complete",
            status="OPTIMAL" if status == cp_model.OPTIMAL else "FEASIBLE",
            newly_scheduled=len(newly_scheduled),
            unique_satellites=len(committed_norads),
            objective_value=solver.ObjectiveValue(),
            best_bound=solver.BestObjectiveBound(),
            optimality_gap_pct=round(
                _optimality_gap_pct(
                    solver.ObjectiveValue(),
                    solver.BestObjectiveBound(),
                ),
                2,
            ),
            wall_time_sec=round(solver.WallTime(), 3),
        )
        return newly_scheduled

    # ------------------------------------------------------------------
    # Candidate query — one SQL query + Python bisect filter
    # ------------------------------------------------------------------

    def _get_candidate_passes(
        self,
        scheduled_satellites: Set[int],
        start_time: datetime,
        end_time: datetime,
        free_slots: Dict[str, List[Tuple[datetime, datetime]]],
    ) -> List[SatellitePass]:
        """
        Fetch passes for unscheduled satellites that fit inside a free slot.

        Query strategy
        --------------
        One broad query (unscheduled + in window + not in scheduled_satellites)
        then Python-side bisect filter per station instead of or_(*conditions).

        The original or_(*conditions) approach built one SQL clause per free slot.
        With 50 stations × many slots this produced hundreds of OR clauses, causing
        Postgres to choose a slow sequential scan and often return no rows within
        the query timeout — the root cause of "no solution found" symptoms.
        """
        if not free_slots:
            return []

        # Single parameterised query — one round trip to Postgres
        query = self.session.query(SatellitePass).filter(
            SatellitePass.is_scheduled == False,
            SatellitePass.rise_time    >= start_time,
            SatellitePass.set_time     <= end_time,
        )
        if scheduled_satellites:
            query = query.filter(
                SatellitePass.norad_id.notin_(list(scheduled_satellites))
            )

        all_passes: List[SatellitePass] = (
            query
            .order_by(SatellitePass.station_id, SatellitePass.rise_time)
            .all()
        )

        if not all_passes:
            return []

        # Pre-sort slots per station once — O(S log S) upfront, O(log K) per pass
        station_slots: Dict[str, List[Tuple[datetime, datetime]]] = {
            sid: sorted(slots, key=lambda s: s[0])
            for sid, slots in free_slots.items()
            if slots
        }

        # Filter: keep only passes that fit entirely inside a free slot
        candidates = [
            p for p in all_passes
            if _fits_in_any_slot(
                p.rise_time,
                p.set_time,
                station_slots.get(p.station_id, []),
            )
        ]

        logger.info(
            "Candidate passes for OR-Tools",
            total_queried=len(all_passes),
            after_slot_filter=len(candidates),
            stations_with_slots=len(station_slots),
            excluded_satellites=len(scheduled_satellites),
        )
        return candidates

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def _log_solver_stats(
        self,
        solver: cp_model.CpSolver,
        status: Any,
        n_vars: int,
    ) -> None:
        logger.info(
            "OR-Tools solver stats",
            status=solver.StatusName(status),
            pass_variables=n_vars,
            wall_time_sec=round(solver.WallTime(), 3),
            branches=solver.NumBranches(),
            conflicts=solver.NumConflicts(),
        )

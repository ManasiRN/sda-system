"""
SGP4 propagation engine — wraps Skyfield's EarthSatellite for orbit propagation.

Two-layer design
----------------
Layer 1 (positions)  : Skyfield EarthSatellite.at() — GCRS km with automatic
                       UT1-UTC and leap-second corrections from IERS data.
Layer 2 (error codes): python-sgp4 Satrec.sgp4_array() — per-step SGP4 error
                       codes so decay, divergence, and element failures are
                       classified rather than silently replaced with NaN.

Coordinate note
---------------
Skyfield's .position.km returns GCRS, NOT TEME.  The original file said "TEME"
— that was wrong.  The transform chain (GCRS→ECEF→ENU) in coordinates.py is
correct as-is.  GCRS/TEME difference is < 0.001° at a 10° elevation mask.

Caching
-------
EarthSatellite construction initialises SGP4 state (~0.5 ms per object).
An OrderedDict-based LRU cache (keyed on SHA-256(line1+line2)) avoids repeated
construction while ensuring frequently-used satellites are not evicted.

Julian date accuracy
--------------------
JD is computed from UTC year/month/day/h/m/s via the Fliegel-Van Flandern
formula (same algorithm as sgp4.api.jday).  This avoids the POSIX-timestamp
path, which has a 1-second ambiguity during UTC leap-second insertions because
POSIX repeats the last second while Julian Date must remain continuous.

Requires python-sgp4 >= 2.20  (Satrec.sgp4_array vectorised C extension).
"""
from __future__ import annotations

import hashlib
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterator, List, Optional, Tuple

import numpy as np
from skyfield.api import EarthSatellite
from structlog import get_logger

from ..config import config
from .coordinates import _ts   # reuse the process-level Skyfield Timescale singleton

logger = get_logger()

# ---------------------------------------------------------------------------
# SGP4 error-code catalogue  (Vallado 2013 §3.7 / python-sgp4 source)
# ---------------------------------------------------------------------------
_SGP4_ERRORS: Dict[int, str] = {
    1: "mean eccentricity out of range [−1e-3, 1)",
    2: "mean motion ≤ 0 — degenerate / parabolic orbit",
    3: "perturbed eccentricity not in [0, 1)",
    4: "semi-latus rectum < 0",
    5: "epoch too old — deep-space anomaly > 20 days from current epoch",
    6: "satellite has decayed — atmospheric re-entry detected",
}

# Warn when TLE epoch is older than this; accuracy degrades rapidly beyond 7 days
_MAX_TLE_AGE_DAYS: float = 7.0

# ---------------------------------------------------------------------------
# LRU EarthSatellite cache  (OrderedDict — insertion order == access order)
# ---------------------------------------------------------------------------
_sat_cache: OrderedDict[str, EarthSatellite] = OrderedDict()
_sat_cache_lock = threading.Lock()
_SAT_CACHE_MAXSIZE: int = 2_000   # ~2 k Satrec objects ≈ 10 MB RAM


# ===========================================================================
# Result type
# ===========================================================================

@dataclass(slots=True)
class PropagationResult:
    """
    Complete outcome of one SGP4 propagation run.

    Callers MUST call ``is_usable()`` before passing positions to the pass
    detector.  Using ``.positions`` directly risks processing NaN rows from
    decayed or diverged satellites.

    Attributes
    ----------
    norad_id     : NORAD catalogue number.
    positions    : shape (N, 3) GCRS km.  NaN rows mark propagation failures.
    times        : length-N list of UTC datetimes aligned with ``positions``.
    valid_mask   : shape (N,) bool — True where the step succeeded.
    error_codes  : shape (N,) int32 — raw SGP4 code per step (0 = no error).
    tle_age_days : TLE epoch age relative to start_time in days.
    decayed      : True if any step returned SGP4 error 6 (atmospheric re-entry).
    """
    norad_id:     int
    positions:    np.ndarray        # (N, 3)  GCRS km
    times:        List[datetime]
    valid_mask:   np.ndarray        # (N,)    bool
    error_codes:  np.ndarray        # (N,)    int32
    tle_age_days: float
    decayed:      bool

    @property
    def valid_positions(self) -> np.ndarray:
        """Rows of ``positions`` where propagation succeeded (finite, error=0)."""
        return self.positions[self.valid_mask]

    @property
    def valid_times(self) -> List[datetime]:
        """Times corresponding to valid positions."""
        return [t for t, v in zip(self.times, self.valid_mask) if v]

    @property
    def valid_fraction(self) -> float:
        """Fraction of timesteps with valid positions [0.0 – 1.0]."""
        n = len(self.valid_mask)
        return float(self.valid_mask.sum()) / n if n else 0.0

    def is_usable(self, min_valid_fraction: float = 0.90) -> bool:
        """
        True if the result has enough valid steps for pass detection.

        Decayed satellites (error 6) are always rejected.  A threshold < 1.0
        allows partially-decayed objects with useful early-window passes.
        """
        return (not self.decayed) and self.valid_fraction >= min_valid_fraction


# ===========================================================================
# Internal helpers
# ===========================================================================

def _to_utc(dt: datetime) -> datetime:
    """
    Normalise any datetime to UTC-aware.

    - Already UTC-aware  → returned as-is (fast path).
    - Non-UTC-aware      → converted with astimezone(utc) to respect the
                           original tzinfo offset (not just relabelled).
    - Naive (no tzinfo)  → labelled as UTC with replace() — callers guarantee
                           their naive datetimes are already UTC.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    if dt.utcoffset() == timedelta(0):
        return dt                                   # already UTC, zero-copy
    return dt.astimezone(timezone.utc)              # e.g. +05:30 → UTC


def _validate_tle_format(line1: str, line2: str) -> Optional[str]:
    """
    Lightweight pre-flight TLE format check before passing to Skyfield.

    Full checksum / field-range validation is TLEValidator's job.  This guard
    catches obviously corrupt data that reaches the engine post-ingestion:
    swapped lines, truncated rows, NORAD ID mismatches.

    Returns None if valid, or an error string describing the problem.
    """
    l1, l2 = line1.strip(), line2.strip()

    if len(l1) != 69:
        return f"line 1 is {len(l1)} chars (expected 69)"
    if len(l2) != 69:
        return f"line 2 is {len(l2)} chars (expected 69)"
    if l1[0] != "1":
        return f"line 1 must start with '1' (got {l1[0]!r})"
    if l2[0] != "2":
        return f"line 2 must start with '2' (got {l2[0]!r})"
    if l1[2:7] != l2[2:7]:
        return f"NORAD ID mismatch: line1={l1[2:7]!r} vs line2={l2[2:7]!r}"

    # Epoch field in line1 columns 19-32 must parse as a float
    epoch_field = l1[18:32].strip()
    try:
        float(epoch_field)
    except ValueError:
        return f"non-numeric epoch field in line 1: {epoch_field!r}"

    return None


def _get_or_create_satellite(line1: str, line2: str, name: str) -> EarthSatellite:
    """
    Return a cached EarthSatellite, building and caching one if absent.

    LRU eviction: every cache hit moves the entry to the end of the
    OrderedDict; eviction removes from the front (least recently used).
    Thread-safe via module-level lock.
    """
    key = hashlib.sha256(
        f"{line1.strip()}|{line2.strip()}".encode()
    ).hexdigest()

    with _sat_cache_lock:
        if key in _sat_cache:
            _sat_cache.move_to_end(key)          # LRU: mark as recently used
            return _sat_cache[key]

        sat = EarthSatellite(line1, line2, name, _ts)
        _sat_cache[key] = sat
        if len(_sat_cache) > _SAT_CACHE_MAXSIZE:
            _sat_cache.popitem(last=False)       # evict least recently used
        return sat


def _tle_epoch(sat: EarthSatellite) -> datetime:
    """
    Extract the TLE epoch from the satellite's Satrec model as a UTC datetime.

    Satrec.epochyr uses a 2-digit year.  NORAD / Celestrak convention:
      00–56 → 2000–2056   (post-2000 objects)
      57–99 → 1957–1999   (Sputnik era through Y2K)
    Satrec.epochdays is the fractional day-of-year (1.0 = Jan 1 00:00:00 UTC).
    """
    yr2  = int(sat.model.epochyr)
    year = (2000 + yr2) if yr2 < 57 else (1900 + yr2)
    return (
        datetime(year, 1, 1, tzinfo=timezone.utc)
        + timedelta(days=sat.model.epochdays - 1.0)
    )


def _jd_from_components(
    years:   np.ndarray,
    months:  np.ndarray,
    days:    np.ndarray,
    hours:   np.ndarray,
    minutes: np.ndarray,
    secs:    np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Vectorized UTC component → split Julian Date (whole, fraction).

    Uses the Fliegel-Van Flandern formula, identical to sgp4.api.jday():
      JD = 367Y − ⌊7(Y+⌊(M+9)/12⌋)/4⌋ + ⌊275M/9⌋ + D + 1721013.5 + UT/24

    Valid for all post-1900 dates (the sign() correction term in the original
    formula is 0 for 100*Y + M > 190002.5, i.e., any date after 1900-02).

    WHY this instead of unix-timestamp / 86400 + 2440587.5:
      POSIX time repeats the last second during a UTC leap-second insertion
      (e.g., 23:59:60 → 23:59:59 again), causing a 1-second ambiguity in the
      converted Julian Date.  The Fliegel-Van Flandern formula works directly
      from UTC calendar fields and stays continuous through leap seconds.

    Returns
    -------
    (jd_whole, jd_frac), each shape (N,) float64.
    Splitting into whole + fraction preserves ~µs precision around JD 2451545.
    """
    Y = years.astype(np.int64)
    M = months.astype(np.int64)
    D = days.astype(np.int64)

    # Integer day number (floor-division throughout)
    jd_int = (
        367 * Y
        - (7 * (Y + (M + 9) // 12)) // 4
        + (275 * M) // 9
        + D
        + np.int64(1_721_013)          # constant epoch offset for this formula
    )

    # Fractional day.  JD epochs start at noon, so midnight = 0.5 offset.
    day_frac = (
        hours.astype(np.float64)   / 24.0
        + minutes.astype(np.float64) / 1_440.0
        + secs                       / 86_400.0
        + 0.5
    )

    floor_frac = np.floor(day_frac)
    jd_whole   = jd_int.astype(np.float64) + floor_frac
    jd_frac    = day_frac - floor_frac
    return jd_whole, jd_frac


# ===========================================================================
# SGP4Engine
# ===========================================================================

class SGP4Engine:
    """
    SGP4 propagation engine.

    Thread-safe — no mutable instance state.  All satellite objects are cached
    at module level under a lock.  Use the module-level singleton ``sgp4_engine``.
    """

    # -----------------------------------------------------------------------
    # Primary API
    # -----------------------------------------------------------------------

    def propagate(
        self,
        line1: str,
        line2: str,
        norad_id: int,
        start_time: datetime,
        name: str = "",
        days: Optional[int] = None,
    ) -> PropagationResult:
        """
        Propagate one satellite's orbit via SGP4 over a time window.

        Parameters
        ----------
        line1, line2 : TLE lines (exactly 69 chars each).
        norad_id     : NORAD catalogue number (used in logs and result).
        start_time   : Window start.  Any timezone is accepted and converted to
                       UTC internally; naive datetimes are assumed to be UTC.
        name         : Human-readable name (optional).
        days         : Window length in days.  Defaults to config.PROPAGATION_DAYS.

        Returns
        -------
        PropagationResult — always returned; never raises on SGP4 runtime errors
        (those are captured in error_codes / NaN rows).  Raises ValueError only
        for structurally invalid TLE lines.
        """
        if days is None:
            days = config.PROPAGATION_DAYS

        # Normalise start_time to UTC-aware (handles naive, UTC, and offset zones)
        start_time = _to_utc(start_time)

        # Pre-flight TLE format check
        fmt_err = _validate_tle_format(line1, line2)
        if fmt_err:
            raise ValueError(f"TLE format error for NORAD {norad_id}: {fmt_err}")

        sat_name = name or str(norad_id)
        sat      = _get_or_create_satellite(line1, line2, sat_name)

        # TLE age warning
        tle_epoch_dt = _tle_epoch(sat)
        tle_age_days = (start_time - tle_epoch_dt).total_seconds() / 86_400.0
        if tle_age_days > _MAX_TLE_AGE_DAYS:
            logger.warning(
                "TLE epoch is stale — accuracy degraded",
                norad_id=norad_id,
                tle_age_days=round(tle_age_days, 2),
                max_recommended_days=_MAX_TLE_AGE_DAYS,
                tle_epoch=tle_epoch_dt.isoformat(),
            )
        elif tle_age_days < 0:
            logger.warning(
                "TLE epoch is in the future relative to start_time",
                norad_id=norad_id,
                tle_age_days=round(tle_age_days, 2),
            )

        # Build time grid (all entries are UTC-aware, inheriting start_time's tzinfo)
        n_steps = int(days * 86_400 / config.PROPAGATION_STEP_SECONDS)
        times: List[datetime] = [
            start_time + timedelta(seconds=i * config.PROPAGATION_STEP_SECONDS)
            for i in range(n_steps + 1)
        ]
        n = len(times)

        # --- Build UTC component arrays (shared by Skyfield + JD computation) ---
        years   = np.fromiter((t.year   for t in times), dtype=np.int32,   count=n)
        months  = np.fromiter((t.month  for t in times), dtype=np.int32,   count=n)
        days_   = np.fromiter((t.day    for t in times), dtype=np.int32,   count=n)
        hours   = np.fromiter((t.hour   for t in times), dtype=np.int32,   count=n)
        minutes = np.fromiter((t.minute for t in times), dtype=np.int32,   count=n)
        secs    = np.fromiter(
            (t.second + t.microsecond * 1e-6 for t in times),
            dtype=np.float64, count=n,
        )

        # --- Layer 1: Skyfield → GCRS positions ---
        sky_times = _ts.utc(years, months, days_, hours, minutes, secs)  # type: ignore[arg-type]
        geocentric = sat.at(sky_times)
        # np.asarray() pins the type to ndarray (Skyfield stubs type .km as
        # a reify descriptor whose return type Pylance cannot infer as ndarray).
        positions_gcrs: np.ndarray = np.asarray(geocentric.position.km).T.copy()  # (N,3)

        # --- Layer 2: per-step SGP4 error codes via Satrec.sgp4_array ---
        # JD computed from UTC components to avoid POSIX leap-second ambiguity.
        jd_whole, jd_frac = _jd_from_components(years, months, days_, hours, minutes, secs)
        try:
            raw_ecodes, _, _ = sat.model.sgp4_array(jd_whole, jd_frac)
            # int32: matches python-sgp4's native error-code dtype; int8 would
            # overflow silently if sgp4 ever returns a code > 127 (defensive).
            error_codes: np.ndarray = np.asarray(raw_ecodes, dtype=np.int32)
        except Exception as exc:
            logger.warning(
                "Satrec.sgp4_array unavailable — falling back to NaN-only detection",
                norad_id=norad_id,
                error=str(exc),
            )
            error_codes = np.zeros(n, dtype=np.int32)

        # Verify error_codes length matches positions (guards against sgp4 API changes)
        if len(error_codes) != n:
            logger.error(
                "sgp4_array returned wrong number of error codes — zeroing array",
                norad_id=norad_id,
                expected=n,
                got=len(error_codes),
            )
            error_codes = np.zeros(n, dtype=np.int32)

        # --- Build validity mask ---
        # Valid iff: SGP4 error code == 0  AND  all three position components finite.
        finite_mask: np.ndarray = np.isfinite(positions_gcrs).all(axis=1)  # (N,) bool
        ok_mask:     np.ndarray = error_codes == 0                          # (N,) bool
        valid_mask:  np.ndarray = finite_mask & ok_mask                     # (N,) bool

        # NaN-fill invalid rows so consumers using .positions directly see clean data
        positions_gcrs[~valid_mask] = np.nan

        # Decay detection (error 6 = atmospheric re-entry)
        decayed = bool(np.any(error_codes == 6))

        # Log every distinct non-zero error code that appeared
        nonzero_codes = set(int(c) for c in error_codes if c != 0)
        for code in sorted(nonzero_codes):
            n_affected = int(np.sum(error_codes == code))
            logger.warning(
                "SGP4 propagation errors detected",
                norad_id=norad_id,
                error_code=code,
                meaning=_SGP4_ERRORS.get(code, "undocumented code"),
                affected_steps=n_affected,
                total_steps=n,
            )

        result = PropagationResult(
            norad_id=norad_id,
            positions=positions_gcrs,
            times=times,
            valid_mask=valid_mask,
            error_codes=error_codes,
            tle_age_days=tle_age_days,
            decayed=decayed,
        )

        logger.debug(
            "SGP4 propagation complete",
            norad_id=norad_id,
            name=sat_name,
            total_steps=n,
            valid_steps=int(valid_mask.sum()),
            valid_pct=f"{result.valid_fraction * 100:.1f}%",
            tle_age_days=round(tle_age_days, 2),
            decayed=decayed,
        )
        return result

    # -----------------------------------------------------------------------
    # Batch API — generator to avoid O(N_sats × N_steps) memory accumulation
    # -----------------------------------------------------------------------

    def batch_propagate(
        self,
        tles: List[Tuple[int, str, str]],
        start_time: datetime,
        names: Optional[Dict[int, str]] = None,
    ) -> Iterator[Tuple[int, Optional[PropagationResult]]]:
        """
        Generator that propagates satellites one at a time, yielding each result
        immediately instead of accumulating all results in memory.

        Memory note
        -----------
        For 9,000 satellites × 60,481 steps (7 days @ 10 s) the positions array
        alone is ~1.4 MB per satellite (~13 GB total if collected into a dict).
        Yielding results one at a time lets the caller process and discard each
        PropagationResult before the next propagation runs, keeping peak memory
        to O(1 satellite).

        Parameters
        ----------
        tles       : list of (norad_id, line1, line2) tuples.
        start_time : propagation window start (any timezone; converted to UTC).
        names      : optional {norad_id: name} for logging.

        Yields
        ------
        (norad_id, PropagationResult) on success.
        (norad_id, None) if the TLE was structurally invalid (ValueError raised
        by propagate).  SGP4 runtime failures are inside PropagationResult —
        they never produce a None yield.
        """
        if names is None:
            names = {}

        n_total = n_failed = n_usable = n_decayed = 0

        for norad_id, line1, line2 in tles:
            n_total += 1
            try:
                result = self.propagate(
                    line1=line1,
                    line2=line2,
                    norad_id=norad_id,
                    start_time=start_time,
                    name=names.get(norad_id, ""),
                )
                if result.is_usable():
                    n_usable += 1
                if result.decayed:
                    n_decayed += 1
                yield norad_id, result
            except Exception as exc:
                logger.error(
                    "Propagation failed — satellite skipped",
                    norad_id=norad_id,
                    error=str(exc),
                )
                n_failed += 1
                yield norad_id, None

        logger.info(
            "Batch propagation complete",
            total=n_total,
            usable=n_usable,
            decayed=n_decayed,
            failed=n_failed,
        )

    # -----------------------------------------------------------------------
    # Cache management
    # -----------------------------------------------------------------------

    def clear_cache(self) -> int:
        """Evict all entries from the satellite object cache."""
        with _sat_cache_lock:
            count = len(_sat_cache)
            _sat_cache.clear()
        logger.info("Satellite object cache cleared", evicted=count)
        return count

    def cache_stats(self) -> Dict[str, int]:
        """Non-blocking snapshot of current cache state."""
        with _sat_cache_lock:
            size = len(_sat_cache)
        return {"size": size, "max_size": _SAT_CACHE_MAXSIZE}


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
sgp4_engine = SGP4Engine()

"""
TLE (Two-Line Element set) validator — production-grade.

Validates format, checksum, field ranges, and cross-line consistency.
Handles both classic 5-digit NORAD IDs and the alpha-5 format
(A0000–Z9999 → 100000–359999) introduced by Space-Track in 2020 for
objects numbered beyond 99999.

TLE column layout — all indices 0-based, lines are exactly 69 characters:

  Line 1:
    0       Line number ('1')
    1       Space
    2:7     Satellite number (5 chars, may be alpha-5)
    7       Classification  (U / C / S)
    8       Space
    9:17    International Designator (8 chars)
    17      Space
    18:20   Epoch year (2-digit)
    20:32   Epoch day-of-year + decimal fraction (12 chars)
    32      Space
    33:43   First deriv of mean motion (ballistic coeff, 10 chars)
    43      Space
    44:52   Second deriv of mean motion (8 chars)
    52      Space
    53:61   BSTAR drag term (8 chars)
    61      Space
    62      Ephemeris type
    63      Space
    64:68   Element set number (4 chars)
    68      Checksum digit

  Line 2:
    0       Line number ('2')
    1       Space
    2:7     Satellite number (must match line 1)
    7       Space
    8:16    Inclination, deg (8 chars, 0–180)
    16      Space
    17:25   RAAN, deg (8 chars, 0–360)
    25      Space
    26:33   Eccentricity, no decimal (7 chars, implied 0.NNNNNNN)
    33      Space
    34:42   Argument of perigee, deg (8 chars, 0–360)
    42      Space
    43:51   Mean anomaly, deg (8 chars, 0–360)
    51      Space
    52:63   Mean motion, rev/day (11 chars)
    63:68   Revolution number at epoch (5 chars)
    68      Checksum digit

NORAD checksum algorithm (applied to first 68 chars):
  - Each digit contributes its face value
  - '-' (minus sign) contributes 1
  - All other characters contribute 0
  - Result = total mod 10
"""
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

from structlog import get_logger

logger = get_logger()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TLE_LINE_LENGTH = 69

# Year cutoff: Sputnik launched Oct 1957, so no satellite predates year 57.
# Two-digit years 57–99 → 1957–1999; 00–56 → 2000–2056.
_EPOCH_YEAR_CUTOFF = 57

# Mean motion bounds: anything outside (0, 20) rev/day is physically wrong
# for an Earth-orbiting object tracked by NORAD.
_MEAN_MOTION_MIN = 0.0
_MEAN_MOTION_MAX = 20.0


# ---------------------------------------------------------------------------
# Module-level private helpers — computed once, reused by every call
# ---------------------------------------------------------------------------

def _tle_line_checksum(line: str) -> int:
    """
    Compute the NORAD mod-10 checksum over the first 68 characters.

    Only the first 68 chars are summed — the 69th char IS the checksum digit.
    Algorithm: digits add their face value; '-' adds 1; everything else adds 0.
    """
    total = 0
    for ch in line[:68]:
        if ch.isdigit():
            total += int(ch)
        elif ch == '-':
            total += 1
    return total % 10


def _parse_satellite_number(field: str) -> Optional[int]:
    """
    Parse a 5-character satellite-number field.

    Classic numeric  : '25544'  → 25544
    Alpha-5 format   : 'A1234'  → 101234  (A=10, B=11 … Z=35)
                       'Z9999'  → 359999
    Returns None for any field that does not match either format.
    """
    s = field.strip()
    if not s:
        return None

    if s.isdigit():
        val = int(s)
        # NORAD IDs start at 1; 0 is not a valid ID
        return val if val > 0 else None

    # Alpha-5: exactly 5 chars, first is uppercase letter, rest are digits
    if len(s) == 5 and s[0].isupper() and s[1:].isdigit():
        letter_val = ord(s[0]) - ord('A') + 10   # A→10, B→11, … Z→35
        return letter_val * 10_000 + int(s[1:])

    return None


# ---------------------------------------------------------------------------
# Public validator class
# ---------------------------------------------------------------------------

class TLEValidator:
    """
    Production-grade TLE format validator.

    All public methods are static — no instance state needed.  Call
    ``validate_pair()`` as the single entry point for batch ingestion:
    it validates both lines, checksums, and the cross-line NORAD ID match
    in one call rather than requiring callers to chain three separate calls.

    ``extract_epoch()`` returns a timezone-aware UTC datetime.
    ``compute_pair_hash()`` returns a SHA-256 hex digest (64 chars).
    """

    # ------------------------------------------------------------------
    # Primary entry point for ingestion
    # ------------------------------------------------------------------

    @staticmethod
    def validate_pair(
        name: str, line1: str, line2: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate a complete TLE triple (name + two lines).

        Checks performed in order:
          1. Line 1 length, structure, epoch range, and checksum
          2. Line 2 length, structure, orbital element ranges, and checksum
          3. NORAD ID must match between line 1 and line 2

        Use this instead of calling validate_line1 + validate_line2 separately
        so the cross-validation (step 3) is never accidentally skipped.
        """
        ok1, err1 = TLEValidator.validate_line1(line1)
        if not ok1:
            return False, f"[{name}] Line 1 invalid: {err1}"

        ok2, err2 = TLEValidator.validate_line2(line2)
        if not ok2:
            return False, f"[{name}] Line 2 invalid: {err2}"

        id1 = _parse_satellite_number(line1[2:7])
        id2 = _parse_satellite_number(line2[2:7])
        if id1 != id2:
            return False, (
                f"[{name}] NORAD ID mismatch between lines: "
                f"line 1 has {id1}, line 2 has {id2} — "
                f"likely a misaligned 3-line block in source data"
            )

        return True, None

    # ------------------------------------------------------------------
    # Individual line validators (kept for backward compatibility)
    # ------------------------------------------------------------------

    @staticmethod
    def validate_line1(line1: str) -> Tuple[bool, Optional[str]]:
        """
        Validate TLE line 1: length, line-number prefix, structure, and checksum.

        Also strips a trailing \\r so callers do not need to handle
        Windows CRLF line endings explicitly — a common source of
        'length = 70, expected 69' failures.
        """
        if not line1:
            return False, "Line 1 is empty"

        # Normalise: strip trailing carriage return from CRLF sources
        line1 = line1.rstrip('\r')

        if len(line1) != TLE_LINE_LENGTH:
            return False, (
                f"Line 1 must be {TLE_LINE_LENGTH} chars, got {len(line1)}"
            )

        if line1[0] != '1':
            return False, f"Line 1 must begin with '1', got {line1[0]!r}"

        if line1[1] != ' ':
            return False, "Column 2 of line 1 must be a space"

        # Satellite number (cols 2:7)
        if _parse_satellite_number(line1[2:7]) is None:
            return False, (
                f"Unrecognised satellite number format: {line1[2:7]!r} — "
                f"expected 5 digits or alpha-5 (letter + 4 digits)"
            )

        # Epoch year (cols 18:20) — must be 2 numeric digits
        epoch_year_str = line1[18:20]
        if not epoch_year_str.isdigit():
            return False, f"Epoch year field is not numeric: {epoch_year_str!r}"

        # Epoch day (cols 20:32) — must be parseable float in [1, 367)
        epoch_day_str = line1[20:32].strip()
        try:
            day = float(epoch_day_str)
        except ValueError:
            return False, f"Epoch day field not parseable as float: {epoch_day_str!r}"

        if not (1.0 <= day < 367.0):
            return False, (
                f"Epoch day {day} out of physical range [1, 367) — "
                f"check for source data corruption"
            )

        # Checksum
        computed = _tle_line_checksum(line1)
        checksum_char = line1[68]
        if not checksum_char.isdigit():
            return False, f"Checksum character is not a digit: {checksum_char!r}"

        expected = int(checksum_char)
        if computed != expected:
            return False, (
                f"Line 1 checksum mismatch: computed {computed}, "
                f"expected {expected}"
            )

        return True, None

    @staticmethod
    def validate_line2(line2: str) -> Tuple[bool, Optional[str]]:
        """
        Validate TLE line 2: length, structure, orbital element ranges,
        and checksum.

        Orbital elements are checked for physical plausibility so that
        corrupted data is caught here rather than crashing the SGP4
        propagator later.
        """
        if not line2:
            return False, "Line 2 is empty"

        line2 = line2.rstrip('\r')

        if len(line2) != TLE_LINE_LENGTH:
            return False, (
                f"Line 2 must be {TLE_LINE_LENGTH} chars, got {len(line2)}"
            )

        if line2[0] != '2':
            return False, f"Line 2 must begin with '2', got {line2[0]!r}"

        if line2[1] != ' ':
            return False, "Column 2 of line 2 must be a space"

        if _parse_satellite_number(line2[2:7]) is None:
            return False, f"Unrecognised satellite number in line 2: {line2[2:7]!r}"

        # --- Orbital element range checks ---

        # Inclination [0, 180] degrees
        try:
            inclination = float(line2[8:16])
        except ValueError:
            return False, f"Inclination not parseable: {line2[8:16]!r}"
        if not (0.0 <= inclination <= 180.0):
            return False, f"Inclination {inclination}° outside physical range [0, 180]"

        # RAAN [0, 360) degrees
        try:
            raan = float(line2[17:25])
        except ValueError:
            return False, f"RAAN not parseable: {line2[17:25]!r}"
        if not (0.0 <= raan < 360.0):
            return False, f"RAAN {raan}° outside physical range [0, 360)"

        # Eccentricity [0, 1) — stored as 7-digit integer with implied '0.'
        ecc_raw = line2[26:33]
        if not ecc_raw.isdigit():
            return False, f"Eccentricity field must be 7 digits, got {ecc_raw!r}"
        eccentricity = float("0." + ecc_raw)
        if not (0.0 <= eccentricity < 1.0):
            return False, (
                f"Eccentricity {eccentricity} ≥ 1.0 — hyperbolic orbit, "
                f"not tracked by NORAD in this catalog"
            )

        # Argument of perigee [0, 360)
        try:
            arg_perigee = float(line2[34:42])
        except ValueError:
            return False, f"Argument of perigee not parseable: {line2[34:42]!r}"
        if not (0.0 <= arg_perigee < 360.0):
            return False, f"Argument of perigee {arg_perigee}° outside [0, 360)"

        # Mean anomaly [0, 360)
        try:
            mean_anomaly = float(line2[43:51])
        except ValueError:
            return False, f"Mean anomaly not parseable: {line2[43:51]!r}"
        if not (0.0 <= mean_anomaly < 360.0):
            return False, f"Mean anomaly {mean_anomaly}° outside [0, 360)"

        # Mean motion (0, 20) rev/day
        # < 0.05 → decaying/re-entered; > 17 → physically impossible for LEO
        try:
            mean_motion = float(line2[52:63])
        except ValueError:
            return False, f"Mean motion not parseable: {line2[52:63]!r}"
        if not (_MEAN_MOTION_MIN < mean_motion < _MEAN_MOTION_MAX):
            return False, (
                f"Mean motion {mean_motion} rev/day outside physical range "
                f"({_MEAN_MOTION_MIN}, {_MEAN_MOTION_MAX})"
            )

        # Checksum
        computed = _tle_line_checksum(line2)
        checksum_char = line2[68]
        if not checksum_char.isdigit():
            return False, f"Checksum character is not a digit: {checksum_char!r}"
        expected = int(checksum_char)
        if computed != expected:
            return False, (
                f"Line 2 checksum mismatch: computed {computed}, "
                f"expected {expected}"
            )

        return True, None

    # ------------------------------------------------------------------
    # Field extraction
    # ------------------------------------------------------------------

    @staticmethod
    def extract_norad_id(line1: str) -> Optional[int]:
        """
        Extract the NORAD catalog ID from TLE line 1 (columns 2:7).

        Handles both the classic 5-digit numeric format and the alpha-5
        format (letter A–Z followed by 4 digits) used for objects beyond
        NORAD ID 99999.
        """
        if len(line1) < 7:
            return None
        return _parse_satellite_number(line1[2:7])

    @staticmethod
    def extract_epoch(line1: str) -> Optional[datetime]:
        """
        Parse the epoch from TLE line 1 into a timezone-aware UTC datetime.

        TLE epoch format (columns 18:32): ``YYDDD.DDDDDDDD``
          - YY  : 2-digit year  (57–99 → 1957–1999; 00–56 → 2000–2056)
          - DDD : day of year, 1-based, with decimal fraction for time-of-day

        Examples
        --------
        ``24001.50000000`` → 2024-01-01 12:00:00 UTC  (day 1, noon)
        ``24366.99999999`` → 2024-12-31 23:59:59 UTC  (last moment of leap year)
        """
        if len(line1) < 32:
            return None

        try:
            year_2d = int(line1[18:20])
            year = 1900 + year_2d if year_2d >= _EPOCH_YEAR_CUTOFF else 2000 + year_2d

            day_of_year = float(line1[20:32].strip())

            if not (1.0 <= day_of_year < 367.0):
                logger.warning(
                    "TLE epoch day out of physical range",
                    year=year,
                    day_of_year=day_of_year,
                    line1_excerpt=line1[18:32],
                )
                return None

            # Day 1.0 = Jan 1 00:00:00 UTC → offset = day - 1
            epoch = datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(
                days=day_of_year - 1.0
            )
            return epoch

        except (ValueError, OverflowError) as exc:
            logger.warning(
                "Failed to parse TLE epoch",
                line1_excerpt=line1[18:32],
                error=str(exc),
            )
            return None

    @staticmethod
    def parse_line2_elements(line2: str) -> Optional[Dict[str, float]]:
        """
        Parse all orbital elements from TLE line 2.

        Returns a dict with float values ready for SGP4 or diagnostics:
          inclination_deg, raan_deg, eccentricity, arg_perigee_deg,
          mean_anomaly_deg, mean_motion_rev_per_day, rev_at_epoch

        Returns None if any field fails to parse (caller should log and skip).
        """
        if not line2 or len(line2) != TLE_LINE_LENGTH:
            return None
        try:
            return {
                "inclination_deg":          float(line2[8:16]),
                "raan_deg":                 float(line2[17:25]),
                "eccentricity":             float("0." + line2[26:33]),
                "arg_perigee_deg":          float(line2[34:42]),
                "mean_anomaly_deg":         float(line2[43:51]),
                "mean_motion_rev_per_day":  float(line2[52:63]),
                "rev_at_epoch":             float(line2[63:68]),
            }
        except ValueError as exc:
            logger.warning("Failed to parse line 2 orbital elements", error=str(exc))
            return None

    # ------------------------------------------------------------------
    # Hashing / fingerprinting
    # ------------------------------------------------------------------

    @staticmethod
    def compute_pair_hash(line1: str, line2: str) -> str:
        """
        Compute a SHA-256 fingerprint of the TLE pair (64 hex chars).

        Used for change detection: if a satellite is re-propagated with
        corrected elements, the hash changes even when the NORAD ID and
        epoch are the same.  SHA-256 replaces MD5 to avoid security-scanner
        noise — this is a content fingerprint, NOT a security MAC.

        Whitespace is normalised before hashing so that CRLF vs LF
        differences in source data do not produce spurious hash mismatches.
        """
        content = f"{line1.strip()}\n{line2.strip()}"
        return hashlib.sha256(content.encode()).hexdigest()

    @staticmethod
    def compute_checksum(line1: str, line2: str) -> str:
        """Alias for compute_pair_hash() — kept for backward compatibility."""
        return TLEValidator.compute_pair_hash(line1, line2)

    # ------------------------------------------------------------------
    # Name validation
    # ------------------------------------------------------------------

    @staticmethod
    def validate_satellite_name(name: str) -> bool:
        """
        Validate a satellite name.

        Accepts any non-empty string of printable ASCII characters
        (0x20–0x7E) up to 100 chars.  This admits real-world names such as:
          'ISS (ZARYA)'  'NOAA-15/R'  'STARLINK-4567+'  'CZ-2D R/B'
        The previous pattern [A-Za-z0-9\\s\\-_]+ incorrectly rejected
        parentheses, slashes, and other common characters in catalog names.
        """
        if not name or len(name) > 100:
            return False
        # All chars must be printable ASCII (space through tilde)
        return all(0x20 <= ord(ch) <= 0x7E for ch in name)

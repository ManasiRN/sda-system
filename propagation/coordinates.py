"""
Coordinate frame transformations for satellite pass detection.

Frame chain
-----------
  GCRS  (Geocentric Celestial Reference System — what Skyfield returns)
    ↓  rotate by GAST around Z axis
  ECEF  (Earth-Centred, Earth-Fixed / ITRS)
    ↓  apply geodetic ENU rotation matrix at ground station
  ENU   (East, North, Up)

All spatial values are in **kilometres** throughout this module.

Accuracy note
-------------
Skyfield's EarthSatellite.at(t).position.km returns GCRS coordinates, not
strict TEME.  The GCRS→ECEF transform used here (single Z-rotation by GAST)
is accurate to < 0.001° in elevation angle — negligible for a 10° elevation
mask.  A full IAU2006 transform (precession + nutation + polar motion) would
add < 1 arcsecond of additional accuracy at the cost of significant complexity.

API
---
Use the *_batch methods in hot paths.  They accept (N, 3) / (N,) arrays and
compute N transforms with O(N) NumPy operations, avoiding 60,480 Python-loop
iterations per satellite-station window.

  coord_converter.teme_to_ecef_batch(positions, times)   → (N,3)
  coord_converter.compute_elevation_azimuth_batch(...)    → (N,), (N,)

The scalar methods are kept for readability and unit tests.
"""
from __future__ import annotations

import numpy as np
from datetime import datetime
from typing import List, Tuple

from skyfield.api import load as _sf_load

# ---------------------------------------------------------------------------
# Skyfield timescale — one instance for the process lifetime.
# Instantiating a new Timescale fetches IERS Earth-orientation data from disk
# and costs ~100 ms.  Do it once at module import, never inside a function.
# ---------------------------------------------------------------------------
_ts = _sf_load.timescale()

# ---------------------------------------------------------------------------
# WGS84 ellipsoid — module-level constants, not class attributes.
# Class attributes can be inadvertently shadowed on instances; module-level
# names cannot.
# ---------------------------------------------------------------------------
_A   = 6_378_137.0              # Semi-major axis (m)
_F   = 1.0 / 298.257_223_563   # Flattening
_B   = _A * (1.0 - _F)          # Semi-minor axis (m)
_E2  = (_A**2 - _B**2) / _A**2 # First eccentricity squared

# GAST is returned in sidereal hours; 1 sidereal hour = π/12 radians
_RAD_PER_SIDEREAL_HOUR: float = np.pi / 12.0


# ===========================================================================
# CoordinateConverter
# ===========================================================================

class CoordinateConverter:
    """
    GCRS ≈ TEME → ECEF → ENU frame transforms.

    All public methods are static — no instance state is needed.
    The module-level singleton ``coord_converter`` is the normal import target.
    """

    # -----------------------------------------------------------------------
    # GCRS → ECEF  (scalar)
    # -----------------------------------------------------------------------

    @staticmethod
    def teme_to_ecef(pos_gcrs_km: np.ndarray, utc_time: datetime) -> np.ndarray:
        """
        Rotate one GCRS position vector (km) into ECEF (km).

        Transform: ECEF = R_z(−GAST) @ GCRS

        R_z(−θ) = [[ cos θ,  sin θ, 0 ],
                   [−sin θ,  cos θ, 0 ],
                   [  0,      0,    1 ]]

        Sign sanity check:
          GAST = 0  → identity (vernal equinox on prime meridian ✓)
          GAST = π/2 → TEME X̂ maps to ECEF −Ŷ (prime meridian is 90° east
                       of vernal equinox, so vernal equinox is at 270°E ✓)
        """
        t = _ts.utc(
            utc_time.year, utc_time.month, utc_time.day,
            utc_time.hour, utc_time.minute,
            utc_time.second + utc_time.microsecond * 1e-6,
        )
        gst: float = float(np.asarray(t.gast).item()) * _RAD_PER_SIDEREAL_HOUR

        cos_g = np.cos(gst)
        sin_g = np.sin(gst)

        R = np.array([
            [ cos_g, sin_g, 0.0],
            [-sin_g, cos_g, 0.0],
            [  0.0,   0.0,  1.0],
        ], dtype=np.float64)

        return R @ pos_gcrs_km

    # -----------------------------------------------------------------------
    # GCRS → ECEF  (batch — use this in production hot paths)
    # -----------------------------------------------------------------------

    @staticmethod
    def teme_to_ecef_batch(
        positions_gcrs_km: np.ndarray,
        times: List[datetime],
    ) -> np.ndarray:
        """
        Vectorized GCRS → ECEF for N positions in one NumPy call.

        Parameters
        ----------
        positions_gcrs_km : ndarray, shape (N, 3)
        times             : list[datetime], length N

        Returns
        -------
        ndarray, shape (N, 3) — ECEF positions (km)

        Performance
        -----------
        Builds one Skyfield Time array for all N timesteps, then applies the
        Z-rotation via broadcasting.  For a 7-day window at 10-second steps
        (N = 60,480) this replaces 60,480 Python iterations with ~8 NumPy
        array operations — roughly 200× faster.
        """
        n = len(times)
        if n == 0:
            return np.empty((0, 3), dtype=np.float64)

        # Build component arrays for one Skyfield ts.utc() call
        years   = np.fromiter((t.year   for t in times), dtype=np.int32,   count=n)
        months  = np.fromiter((t.month  for t in times), dtype=np.int32,   count=n)
        days    = np.fromiter((t.day    for t in times), dtype=np.int32,   count=n)
        hours   = np.fromiter((t.hour   for t in times), dtype=np.int32,   count=n)
        minutes = np.fromiter((t.minute for t in times), dtype=np.int32,   count=n)
        seconds = np.fromiter(
            (t.second + t.microsecond * 1e-6 for t in times),
            dtype=np.float64, count=n,
        )

        sky_t = _ts.utc(years, months, days, hours, minutes, seconds)  # type: ignore[arg-type]

        # GAST in radians, shape (N,)
        gst = np.asarray(sky_t.gast, dtype=np.float64) * _RAD_PER_SIDEREAL_HOUR

        cos_g = np.cos(gst)  # (N,)
        sin_g = np.sin(gst)  # (N,)

        x = positions_gcrs_km[:, 0]
        y = positions_gcrs_km[:, 1]
        z = positions_gcrs_km[:, 2]

        # Apply R_z(−GAST) row-by-row via broadcasting — no explicit loop
        x_ecef =  cos_g * x + sin_g * y
        y_ecef = -sin_g * x + cos_g * y
        z_ecef =  z

        return np.column_stack([x_ecef, y_ecef, z_ecef])

    # -----------------------------------------------------------------------
    # ECEF displacement vector → ENU
    # -----------------------------------------------------------------------

    @staticmethod
    def ecef_to_enu(
        delta_ecef_km: np.ndarray,
        lat_rad: float,
        lon_rad: float,
    ) -> np.ndarray:
        """
        Rotate an ECEF **displacement vector** into the local ENU frame.

        Parameters
        ----------
        delta_ecef_km : ndarray, shape (3,)
            Vector from ground station to satellite in ECEF (km).
            This MUST be a displacement (satellite_pos − station_pos),
            NOT an absolute ECEF position — passing an absolute position
            would produce physically meaningless results.
        lat_rad, lon_rad : float
            Station geodetic latitude / longitude (radians).

        Returns
        -------
        ndarray, shape (3,) — [East, North, Up] (km)

        ENU rotation matrix (standard geodetic derivation):
          E row = [−sin λ,          cos λ,         0        ]
          N row = [−sin φ·cos λ,  −sin φ·sin λ,  cos φ     ]
          U row = [ cos φ·cos λ,   cos φ·sin λ,  sin φ     ]
        where φ = lat_rad, λ = lon_rad.
        """
        sin_lat, cos_lat = np.sin(lat_rad), np.cos(lat_rad)
        sin_lon, cos_lon = np.sin(lon_rad), np.cos(lon_rad)

        R = np.array([
            [-sin_lon,               cos_lon,              0.0    ],
            [-sin_lat * cos_lon,  -sin_lat * sin_lon,  cos_lat   ],
            [ cos_lat * cos_lon,   cos_lat * sin_lon,  sin_lat   ],
        ], dtype=np.float64)

        return R @ delta_ecef_km

    # -----------------------------------------------------------------------
    # Elevation & azimuth  (scalar)
    # -----------------------------------------------------------------------

    @staticmethod
    def compute_elevation_azimuth(
        sat_ecef_km: np.ndarray,
        station_ecef_km: np.ndarray,
        lat_rad: float,
        lon_rad: float,
    ) -> Tuple[float, float]:
        """
        Elevation (deg) and azimuth (deg) from ground station to satellite.

        Returns
        -------
        elevation : float, degrees above horizon [−90, 90]
        azimuth   : float, degrees clockwise from North [0, 360)
        """
        delta = sat_ecef_km - station_ecef_km
        enu = CoordinateConverter.ecef_to_enu(delta, lat_rad, lon_rad)

        rng = float(np.linalg.norm(enu))
        if rng < 1e-9:
            return 0.0, 0.0

        # clip guards against floating-point noise pushing u/rng outside [−1, 1]
        elevation = float(np.degrees(np.arcsin(np.clip(enu[2] / rng, -1.0, 1.0))))
        azimuth   = float(np.degrees(np.arctan2(enu[0], enu[1]))) % 360.0

        return elevation, azimuth

    # -----------------------------------------------------------------------
    # Elevation & azimuth  (batch — use this in production hot paths)
    # -----------------------------------------------------------------------

    @staticmethod
    def compute_elevation_azimuth_batch(
        sat_ecef_km_batch: np.ndarray,
        station_ecef_km: np.ndarray,
        lat_rad: float,
        lon_rad: float,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Vectorized elevation & azimuth for N satellite positions.

        Parameters
        ----------
        sat_ecef_km_batch : ndarray, shape (N, 3)
        station_ecef_km   : ndarray, shape (3,)
        lat_rad, lon_rad  : float — station geodetic coords (radians)

        Returns
        -------
        elevations : ndarray, shape (N,), degrees [−90, 90]
        azimuths   : ndarray, shape (N,), degrees [0, 360)

        Implementation
        --------------
        Applies the 3×3 ENU rotation matrix to all N displacement vectors
        simultaneously using broadcasting — no explicit Python loop.
        clip() prevents NaN from floating-point noise when u/range ≈ ±1.
        """
        # (N, 3) displacement vectors from station to each satellite position
        deltas = sat_ecef_km_batch - station_ecef_km  # broadcasting: (N,3) - (3,)

        sin_lat, cos_lat = np.sin(lat_rad), np.cos(lat_rad)
        sin_lon, cos_lon = np.sin(lon_rad), np.cos(lon_rad)

        # ENU components via broadcasting (no loop, no matrix allocation)
        east  = (-sin_lon           * deltas[:, 0]
                 + cos_lon           * deltas[:, 1])

        north = (-sin_lat * cos_lon  * deltas[:, 0]
                 - sin_lat * sin_lon  * deltas[:, 1]
                 + cos_lat            * deltas[:, 2])

        up    = ( cos_lat * cos_lon  * deltas[:, 0]
                + cos_lat * sin_lon  * deltas[:, 1]
                + sin_lat            * deltas[:, 2])

        ranges = np.sqrt(east**2 + north**2 + up**2)

        # Mask near-zero ranges to avoid division by zero
        valid      = ranges > 1e-9
        safe_range = np.where(valid, ranges, 1.0)

        elevations = np.where(
            valid,
            np.degrees(np.arcsin(np.clip(up / safe_range, -1.0, 1.0))),
            0.0,
        )
        azimuths = np.where(
            valid,
            np.degrees(np.arctan2(east, north)) % 360.0,
            0.0,
        )

        return elevations, azimuths

    # -----------------------------------------------------------------------
    # Geodetic → ECEF  (WGS84)
    # -----------------------------------------------------------------------

    @staticmethod
    def station_to_ecef(lat_deg: float, lon_deg: float, alt_m: float) -> np.ndarray:
        """
        Convert WGS84 geodetic coordinates to ECEF (km).

        Uses the prime-vertical radius of curvature N:
          N = a / sqrt(1 − e²·sin²φ)
          x = (N + h)·cosφ·cosλ
          y = (N + h)·cosφ·sinλ
          z = (N·(1 − e²) + h)·sinφ

        Parameters
        ----------
        lat_deg : float, geodetic latitude  [−90,  90] degrees
        lon_deg : float, geodetic longitude [−180, 180] degrees
        alt_m   : float, altitude above WGS84 ellipsoid (metres)

        Returns
        -------
        ndarray, shape (3,), ECEF position in kilometres.
        """
        lat = np.radians(lat_deg)
        lon = np.radians(lon_deg)

        sin_lat, cos_lat = np.sin(lat), np.cos(lat)
        sin_lon, cos_lon = np.sin(lon), np.cos(lon)

        # Prime-vertical radius of curvature (metres)
        N = _A / np.sqrt(1.0 - _E2 * sin_lat**2)

        x_m = (N + alt_m) * cos_lat * cos_lon
        y_m = (N + alt_m) * cos_lat * sin_lon
        z_m = (N * (1.0 - _E2) + alt_m) * sin_lat

        return np.array([x_m, y_m, z_m], dtype=np.float64) * 1e-3  # m → km


# ---------------------------------------------------------------------------
# Module-level singleton — the normal import target for other modules:
#   from .coordinates import coord_converter
# ---------------------------------------------------------------------------
coord_converter = CoordinateConverter()

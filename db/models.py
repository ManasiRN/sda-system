"""SQLAlchemy ORM models — SQLAlchemy 2.x declarative style.

Mapped[] columns give full static-type inference: Pylance / mypy know
the exact Python type of every attribute without runtime guessing.

Timestamp strategy: server_default=func.now() lets the DB be the
authoritative clock (avoids skew across multiple app-server instances).
A Python-side default= is also provided so the attribute is populated
before the first DB flush (useful in tests and in-memory assertions).
"""
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _now() -> datetime:
    """Timezone-aware UTC timestamp for Python-side column defaults."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Base — SQLAlchemy 2.x class-based declarative (replaces declarative_base())
# ---------------------------------------------------------------------------
class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# TLE
# ---------------------------------------------------------------------------
class TLE(Base):
    """Two-Line Element data for one satellite at one epoch."""

    __tablename__ = "tles"
    __table_args__ = (
        UniqueConstraint("norad_id", "epoch", name="uix_tles_norad_epoch"),
        Index("idx_tles_norad_current", "norad_id", "is_current"),
        Index("idx_tles_epoch", "epoch"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    norad_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    name: Mapped[Optional[str]] = mapped_column(String(100))
    line1: Mapped[str] = mapped_column(String(100), nullable=False)
    line2: Mapped[str] = mapped_column(String(100), nullable=False)
    epoch: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    checksum: Mapped[Optional[str]] = mapped_column(String(64))  # SHA-256 hex digest
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_now,
        server_default=func.now(),
    )

    def __repr__(self) -> str:
        return (
            f"<TLE norad_id={self.norad_id} name={self.name!r} "
            f"epoch={self.epoch} current={self.is_current}>"
        )


# ---------------------------------------------------------------------------
# GroundStation
# ---------------------------------------------------------------------------
class GroundStation(Base):
    """Fixed ground station that can receive satellite downlinks."""

    __tablename__ = "ground_stations"
    __table_args__ = (
        CheckConstraint("latitude  BETWEEN -90  AND  90",  name="ck_gs_latitude_range"),
        CheckConstraint("longitude BETWEEN -180 AND 180",  name="ck_gs_longitude_range"),
        CheckConstraint("altitude_m >= 0",                 name="ck_gs_altitude_non_negative"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    station_id: Mapped[str] = mapped_column(String(10), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    altitude_m: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    elevation_mask_deg: Mapped[float] = mapped_column(Float, nullable=False, default=10.0)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    def __repr__(self) -> str:
        return (
            f"<GroundStation {self.station_id!r} name={self.name!r} "
            f"lat={self.latitude:.3f} lon={self.longitude:.3f} active={self.is_active}>"
        )


# ---------------------------------------------------------------------------
# SatellitePass
# ---------------------------------------------------------------------------
class SatellitePass(Base):
    """
    One predicted pass of a satellite over a ground station.

    Indexes are chosen to match the three hot query patterns:
      1. Scheduler reads: unscheduled passes per station in a time window
         → idx_passes_station_scheduled_rise
      2. Coverage reads: all passes for a satellite
         → idx_passes_norad_rise
      3. Greedy scorer: sort by max_elevation DESC
         → idx_passes_unscheduled_elevation
    """

    __tablename__ = "satellite_passes"
    __table_args__ = (
        UniqueConstraint(
            "norad_id", "station_id", "rise_time",
            name="uix_passes_norad_station_rise",
        ),
        # Hot path: scheduling reads — station + scheduled flag + time window
        Index("idx_passes_station_scheduled_rise", "station_id", "is_scheduled", "rise_time"),
        # Hot path: coverage reads — satellite history
        Index("idx_passes_norad_rise", "norad_id", "rise_time"),
        # Greedy scorer: pick highest-value unscheduled pass
        Index("idx_passes_unscheduled_elevation", "is_scheduled", "max_elevation"),
        # Coverage aggregation: count by scheduler stage
        Index("idx_passes_scheduled_by", "is_scheduled", "scheduled_by"),
        # DB-level sanity guards
        CheckConstraint("set_time > rise_time",           name="ck_pass_time_order"),
        CheckConstraint("duration_seconds > 0",           name="ck_pass_duration_positive"),
        CheckConstraint("max_elevation BETWEEN 0 AND 90", name="ck_pass_elevation_range"),
        CheckConstraint(
            "scheduled_by IN ('greedy', 'interval_tree', 'ortools') OR scheduled_by IS NULL",
            name="ck_pass_scheduled_by_values",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    norad_id: Mapped[int] = mapped_column(Integer, nullable=False)
    # FK enforces referential integrity: passes for non-existent stations are rejected
    station_id: Mapped[str] = mapped_column(
        String(10),
        ForeignKey("ground_stations.station_id", ondelete="RESTRICT"),
        nullable=False,
    )
    rise_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    set_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    # Float: pass detector returns fractional seconds; Integer would silently truncate
    duration_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    max_elevation: Mapped[float] = mapped_column(Float, nullable=False)
    max_elevation_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    azimuth_at_rise: Mapped[Optional[float]] = mapped_column(Float)
    azimuth_at_set: Mapped[Optional[float]] = mapped_column(Float)
    azimuth_at_max: Mapped[Optional[float]] = mapped_column(Float)
    is_scheduled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, index=True
    )
    # Values constrained by ck_pass_scheduled_by_values above
    scheduled_by: Mapped[Optional[str]] = mapped_column(String(20))
    tle_epoch: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_now,
        server_default=func.now(),
    )

    def __repr__(self) -> str:
        return (
            f"<SatellitePass norad={self.norad_id} station={self.station_id!r} "
            f"rise={self.rise_time} elev={self.max_elevation:.1f}° "
            f"scheduled={self.is_scheduled}>"
        )


# ---------------------------------------------------------------------------
# APIKey
# ---------------------------------------------------------------------------
class APIKey(Base):
    """
    Hashed API key for authenticating requests.

    The raw key is NEVER stored.  Only the SHA-256 hex digest (key_hash)
    is persisted, so a DB breach does not expose working credentials.
    Key rotation: mark is_active=False on the old row and insert a new one.
    """

    __tablename__ = "api_keys"
    __table_args__ = (
        CheckConstraint("length(key_hash) = 64", name="ck_apikey_hash_length"),
        CheckConstraint("rate_limit_per_min > 0", name="ck_apikey_rate_positive"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # SHA-256 hex digest of the raw key — 64 hex chars, indexed for O(1) auth lookup
    key_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    name: Mapped[Optional[str]] = mapped_column(String(100))
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    rate_limit_per_min: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_now,
        server_default=func.now(),
    )
    last_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    def __repr__(self) -> str:
        return (
            f"<APIKey id={self.id} name={self.name!r} "
            f"active={self.is_active} rpm={self.rate_limit_per_min}>"
        )

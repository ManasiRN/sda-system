"""
db/session.py — SQLAlchemy engine + session factory.

Fixes vs. original
------------------
  - db.rollback() added on exception in get_db() — without explicit rollback a
    failed request leaves the session in a dirty state; the connection returned
    to the pool carries uncommitted changes that silently corrupt the next request
    that reuses it.
  - expire_on_commit=False — SQLAlchemy's default (True) expires every attribute
    after commit, forcing a lazy-load round-trip when the response serializer
    reads those objects.  With expire_on_commit=False the already-loaded data is
    served directly; each request still starts with a fresh session so stale-read
    risk across requests is zero.
  - connect_timeout added — without it a hung or unreachable Postgres server
    blocks the worker thread indefinitely; 10 s is a typical production default.
  - application_name added — tags every backend connection in pg_stat_activity
    and pg_locks so DBAs can identify sda_system connections at a glance.
  - pool_timeout lowered to 5 s — under pool exhaustion, fail fast and return
    HTTP 503 rather than queueing requests for the default 30 s and triggering
    cascading timeouts upstream.
  - max_overflow derived from pool_size — total connections cap (pool_size +
    max_overflow) scales with the configured pool so neither over- nor
    under-provisioning occurs when DATABASE_POOL_SIZE is tuned.
"""

from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from ..config import config

# ---------------------------------------------------------------------------
# Connection tuning
# ---------------------------------------------------------------------------
_CONNECT_TIMEOUT_S = 10   # seconds to wait for TCP + Postgres auth
_POOL_TIMEOUT_S    = 5    # seconds to wait for a pool slot before raising

engine = create_engine(
    config.DATABASE_URL.get_secret_value(),
    pool_size=config.DATABASE_POOL_SIZE,
    # max_overflow scales with pool_size — total cap = pool_size + max_overflow
    max_overflow=max(5, config.DATABASE_POOL_SIZE // 2),
    pool_pre_ping=True,    # discard stale connections before handing them out
    pool_recycle=3600,     # recycle connections older than 1 h (avoids idle-timeout drops)
    pool_timeout=_POOL_TIMEOUT_S,
    connect_args={
        "connect_timeout": _CONNECT_TIMEOUT_S,
        "application_name": "sda_system",
    },
)

# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------
# expire_on_commit=False — see module docstring for rationale.
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency — yields one DB session per request.

    Rolls back on any unhandled exception so the connection returned to the
    pool is always in a clean, transaction-free state.  Closes in all cases
    so the pool slot is immediately available for the next request.
    """
    db = SessionLocal()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

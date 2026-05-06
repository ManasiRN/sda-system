"""HTTP API layer — FastAPI application, routes, and middleware."""

from typing import TYPE_CHECKING

# Middleware classes are imported eagerly — they have no startup side effects
# and are needed by non-API code (e.g. tests injecting auth dependencies).
from .middleware import APIKeyAuth, RateLimiter, RequestLoggingMiddleware

# TYPE_CHECKING block - tells type checker these exist (no runtime import)
if TYPE_CHECKING:
    from .main import app

__all__ = [
    "app",
    "APIKeyAuth",
    "RateLimiter",
    "RequestLoggingMiddleware",
]


def __getattr__(name: str):
    """Lazy import of app to avoid Prometheus double-registration."""
    # `app` is lazy-imported to prevent api/main.py executing at package load
    # time.  api/main.py registers Prometheus Counter/Histogram at module level;
    # eager registration raises ValueError("Duplicated timeseries") if any
    # other code path in the same process has already imported api/main.py
    # (e.g. a second test module, a Celery worker that imports middleware).
    if name == "app":
        from .main import app  # noqa: PLC0415
        return app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")



def __dir__() -> list:
    """Return list of available attributes for dir() and IDE autocomplete."""
    return __all__
"""Space Domain Awareness (SDA) Pass Scheduling System.

A production-grade satellite pass scheduling system with two-stage optimization
using greedy algorithm and OR-Tools CP-SAT solver.
"""

__version__ = "1.0.0"
__author__  = "SDA Team"
__license__ = "Proprietary"

# TYPE_CHECKING block - tells type checker these exist (no runtime import)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import settings
    from .propagation.sgp4_engine import sgp4_engine, PropagationResult
    from .propagation.coordinates import coord_converter
    from .propagation.pass_detector import pass_detector
    from .scheduling.greedy import GreedyScheduler
    from .scheduling.interval_tree import IntervalTreeScheduler
    from .cache.redis_client import RedisCache

# Public surface — all names resolved lazily via __getattr__
__all__ = [
    "settings",
    "sgp4_engine",
    "PropagationResult",
    "coord_converter",
    "pass_detector",
    "GreedyScheduler",
    "IntervalTreeScheduler",
    "RedisCache",
]


def __getattr__(name: str):
    """Lazy import of heavy modules only when accessed."""
    if name == "settings":
        from .config import settings
        return settings
    if name == "sgp4_engine":
        from .propagation.sgp4_engine import sgp4_engine
        return sgp4_engine
    if name == "PropagationResult":
        from .propagation.sgp4_engine import PropagationResult
        return PropagationResult
    if name == "coord_converter":
        from .propagation.coordinates import coord_converter
        return coord_converter
    if name == "pass_detector":
        from .propagation.pass_detector import pass_detector
        return pass_detector
    if name == "GreedyScheduler":
        from .scheduling.greedy import GreedyScheduler
        return GreedyScheduler
    if name == "IntervalTreeScheduler":
        from .scheduling.interval_tree import IntervalTreeScheduler
        return IntervalTreeScheduler
    if name == "RedisCache":
        from .cache.redis_client import RedisCache
        return RedisCache
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# Optional: Add __dir__ to help IDEs with autocomplete
def __dir__() -> list:
    """Return list of public attributes for dir() and IDE autocomplete."""
    return __all__
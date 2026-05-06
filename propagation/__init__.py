# sda_system/propagation/__init__.py
"""
Physics layer — SGP4 propagation, coordinate transforms, and pass detection.
"""

from .sgp4_engine import SGP4Engine, PropagationResult, sgp4_engine
from .coordinates import CoordinateConverter, coord_converter
from .pass_detector import PassDetector, pass_detector

__all__ = [
    "SGP4Engine",
    "PropagationResult",   # primary return type of sgp4_engine.propagate();
    "sgp4_engine",         # was missing — type hints in celery_app and tests
    "CoordinateConverter", # had to import directly from propagation.sgp4_engine
    "coord_converter",
    "PassDetector",
    "pass_detector",
]
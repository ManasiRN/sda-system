# sda_system/workers/__init__.py
"""
Task pipeline layer — Celery application and distributed task definitions.
"""

from .celery_app import (
    app,
    ingest_tles,
    propagate_batch,
    detect_passes,
    run_greedy,
    run_ortools,
    check_queue_depth,   # was missing — task exists in beat schedule but was
)                        # not exported, making manual triggering impossible

__all__ = [
    "app",
    "ingest_tles",
    "propagate_batch",
    "detect_passes",
    "run_greedy",
    "run_ortools",
    "check_queue_depth",
]
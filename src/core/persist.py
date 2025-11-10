"""Minimal persist helpers used by driver.py.

This file provides small helper functions so the repo can import `persist`.
You can expand these implementations later to add robust persistence logic.
"""
from typing import Any


def save_parquet(df: Any, path: str, mode: str = "overwrite"):
    """Write a Spark DataFrame to parquet. If df is None this is a no-op."""
    try:
        if df is None:
            return
        df.write.mode(mode).parquet(path)
    except Exception:
        # swallow errors for now; caller should handle/report if needed
        pass


def noop(*args, **kwargs):
    return None


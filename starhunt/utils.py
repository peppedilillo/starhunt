from datetime import datetime
import os

from .exceptions import MissingEnvironmentVariable


def required_env(name: str) -> str:
    if (value := os.environ.get(name)) is None:
        raise MissingEnvironmentVariable(name)
    return value


def is_tz_aware(dt: datetime) -> bool:
    """Returns if a datetime object is timezone-aware."""
    if dt.tzinfo is None or dt.utcoffset() is None:
        return False
    return True

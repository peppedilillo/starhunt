from datetime import datetime


def is_tz_aware(dt: datetime) -> bool:
    """Return whether a datetime is timezone-aware."""
    if dt.tzinfo is None or dt.utcoffset() is None:
        return False
    return True

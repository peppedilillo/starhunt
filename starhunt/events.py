"""Summary read models."""

from dataclasses import dataclass
from datetime import datetime

from .astro import Localization


@dataclass(frozen=True)
class EventSummary:
    """Event summary for list views."""

    id: int
    external_id: str
    created_at: datetime
    last_updated: datetime | None
    notice_count: int
    conesearch_count: int
    latest_burst_datetime: datetime | None
    latest_localization: Localization | None

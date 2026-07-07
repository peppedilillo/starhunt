"""API event summary models."""

from dataclasses import dataclass
from datetime import datetime

from .astro import ConeRegion


@dataclass(frozen=True)
class EventSummary:
    """Public event summary returned by the events endpoint."""

    id: int
    external_id: str
    created_at: datetime
    last_updated: datetime | None
    notice_count: int
    conesearch_count: int
    latest_burst_datetime: datetime | None
    latest_localization: ConeRegion | None

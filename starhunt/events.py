"""API event summary models."""

from dataclasses import dataclass
from datetime import datetime

from .astro import cone_region_from_coordinates
from .astro import ConeRegion
from .db import EventSummaryRow


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


def event_summary_from_row(row: EventSummaryRow) -> EventSummary:
    """Build a public event summary from a database event summary row."""
    return EventSummary(
        id=row.id,
        external_id=row.external_id,
        created_at=row.created_at,
        last_updated=row.last_updated,
        notice_count=row.notice_count,
        conesearch_count=row.conesearch_count,
        latest_burst_datetime=row.latest_burst_datetime,
        latest_localization=cone_region_from_coordinates(
            row.latest_localization_ra,
            row.latest_localization_dec,
            row.latest_localization_err_radius,
        ),
    )

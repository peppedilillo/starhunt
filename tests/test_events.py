from datetime import datetime
from datetime import timezone

from starhunt.astro import ConeRegion
from starhunt.db import EventSummaryRow
from starhunt.events import event_summary_from_row
from starhunt.events import EventSummary


def test_event_summary_from_row_maps_scalar_localization_to_cone_region():
    created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    last_updated = datetime(2026, 1, 2, tzinfo=timezone.utc)
    burst_datetime = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)

    row = EventSummaryRow(
        id=1,
        external_id="Fermi:test",
        created_at=created_at,
        last_updated=last_updated,
        notice_count=2,
        conesearch_count=1,
        latest_burst_datetime=burst_datetime,
        latest_localization_ra=3,
        latest_localization_dec=4,
        latest_localization_err_radius=0.2,
    )

    assert event_summary_from_row(row) == EventSummary(
        id=1,
        external_id="Fermi:test",
        created_at=created_at,
        last_updated=last_updated,
        notice_count=2,
        conesearch_count=1,
        latest_burst_datetime=burst_datetime,
        latest_localization=ConeRegion(ra=3, dec=4, err_radius=0.2),
    )

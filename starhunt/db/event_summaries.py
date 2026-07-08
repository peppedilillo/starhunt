"""Database rows and SQL helpers for event summaries."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class EventSummaryRow:
    """Database event summary row."""

    id: int
    external_id: str
    created_at: datetime
    last_updated: datetime | None
    notice_count: int
    conesearch_count: int
    latest_burst_datetime: datetime | None
    latest_localization_ra: float | None
    latest_localization_dec: float | None
    latest_localization_err_radius: float | None


def get_events_summary(
    cursor,
    *,
    tstart: datetime | None = None,
    tstop: datetime | None = None,
) -> list[EventSummaryRow]:
    """Return event summary rows ordered by creation time.

    ``last_updated`` and ``conesearch_count`` follow timeline semantics:
    notices always count, while cone-searches count only when they returned
    alerts.

    Args:
        cursor: Database cursor.
        tstart: Inclusive created_at lower bound.
        tstop: Exclusive created_at upper bound.

    Returns:
        Event summary rows in creation order, oldest first.
    """
    where_clauses = []
    params = {}
    if tstart is not None:
        where_clauses.append("created_at >= %(tstart)s")
        params["tstart"] = tstart
    if tstop is not None:
        where_clauses.append("created_at < %(tstop)s")
        params["tstop"] = tstop
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    cursor.execute(
        f"""
        WITH filtered_events AS (
            SELECT id, external_id, created_at
            FROM events
            {where_sql}
        ),
        milestones AS (
            SELECT
                notices.event_id,
                notices.published_at AS happened_at,
                'notice' AS milestone_type
            FROM notices
            JOIN filtered_events
                ON filtered_events.id = notices.event_id

            UNION ALL

            SELECT
                conesearches.event_id,
                conesearches.queried_at AS happened_at,
                'conesearch' AS milestone_type
            FROM conesearches
            JOIN filtered_events
                ON filtered_events.id = conesearches.event_id
            WHERE conesearches.alert_count > 0
        ),
        milestone_summary AS (
            SELECT
                event_id,
                max(happened_at) AS last_updated,
                count(*) FILTER (WHERE milestone_type = 'notice') AS notice_count,
                count(*) FILTER (WHERE milestone_type = 'conesearch') AS conesearch_count
            FROM milestones
            GROUP BY event_id
        )
        SELECT
            filtered_events.id,
            filtered_events.external_id,
            filtered_events.created_at,
            milestone_summary.last_updated,
            coalesce(milestone_summary.notice_count, 0) AS notice_count,
            coalesce(milestone_summary.conesearch_count, 0) AS conesearch_count,
            latest_notice.burst_datetime AS latest_burst_datetime,
            latest_localization.ra AS latest_localization_ra,
            latest_localization.dec AS latest_localization_dec,
            latest_localization.err_radius AS latest_localization_err_radius
        FROM filtered_events
        LEFT JOIN milestone_summary
            ON milestone_summary.event_id = filtered_events.id
        LEFT JOIN LATERAL (
            SELECT burst_datetime
            FROM notices
            WHERE notices.event_id = filtered_events.id
            ORDER BY published_at DESC, id DESC
            LIMIT 1
        ) AS latest_notice ON true
        LEFT JOIN LATERAL (
            SELECT ra, dec, err_radius
            FROM notices
            WHERE notices.event_id = filtered_events.id
                AND retracted_by IS NULL
                AND ra IS NOT NULL
                AND dec IS NOT NULL
                AND err_radius IS NOT NULL
            ORDER BY published_at DESC, id DESC
            LIMIT 1
        ) AS latest_localization ON true
        ORDER BY filtered_events.created_at ASC, filtered_events.id ASC
        """,
        params,
    )
    return [EventSummaryRow(*row) for row in cursor.fetchall()]

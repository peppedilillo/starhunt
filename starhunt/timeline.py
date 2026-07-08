"""Timeline response projection helpers."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from .conesearches import conesearch_metadata_from_row
from .conesearches import ConesearchMetadata
from .db import ConesearchRow
from .db import NoticeRow
from .notices import notice_metadata_from_row
from .notices import NoticeMetadata


@dataclass(frozen=True)
class Milestone:
    """Timeline milestone with public notice or cone-search metadata."""

    type: Literal["notice", "conesearch"]
    time: datetime
    content: NoticeMetadata | ConesearchMetadata


def build_event_milestones(notices: list[NoticeRow], conesearches: list[ConesearchRow]) -> list[Milestone]:
    """Build an event timeline from source rows.

    Notice milestones are timed by ``published_at``. Cone-search milestones are
    timed by ``queried_at`` and only emitted when the search returned alerts.
    Milestone content is projected into public API metadata models.

    Args:
        notices: Notice rows for the event.
        conesearches: Cone-search rows for the event.

    Returns:
        Milestones ordered by timeline time, oldest first.
    """
    milestones = [
        Milestone(type="notice", time=notice.published_at, content=notice_metadata_from_row(notice)) for notice in notices
    ]
    milestones.extend(
        Milestone(
            type="conesearch",
            time=conesearch.queried_at,
            content=conesearch_metadata_from_row(conesearch),
        )
        for conesearch in conesearches
        if conesearch.alert_count > 0
    )

    return sorted(
        milestones,
        key=lambda milestone: (
            milestone.time,
            0 if milestone.type == "notice" else 1,
            milestone.content.id,
        ),
    )

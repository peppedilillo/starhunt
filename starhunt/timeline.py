"""Timeline projection helpers."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from .db import Conesearch
from .db import Notice


@dataclass(frozen=True)
class Milestone:
    """Timeline milestone with a common time and typed content."""

    type: Literal["notice", "conesearch"]
    time: datetime
    content: Notice | Conesearch


def build_event_milestones(notices: list[Notice], conesearches: list[Conesearch]) -> list[Milestone]:
    """Build an event timeline from source rows.

    Args:
        notices: Notice rows for the event.
        conesearches: Cone-search rows for the event.

    Returns:
        Milestones ordered by timeline time, oldest first.
    """
    milestones = [Milestone(type="notice", time=notice.published_at, content=notice) for notice in notices]
    milestones.extend(
        Milestone(type="conesearch", time=conesearch.subject_time_start, content=conesearch)
        for conesearch in conesearches
    )

    return sorted(
        milestones,
        key=lambda milestone: (
            milestone.time,
            0 if milestone.type == "notice" else 1,
            milestone.content.id,
        ),
    )

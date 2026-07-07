"""API response models and response-model exports."""

from dataclasses import dataclass
from typing import Any

from ..astro import ConeRegion
from ..conesearches import ConesearchMetadata
from ..events import EventSummary
from ..notices import NoticeMetadata
from ..timeline import Milestone


@dataclass(frozen=True)
class ConesearchResponse:
    """Cone-search metadata and parsed result payload exposed by the API."""

    metadata: ConesearchMetadata
    payload: list[dict[str, Any]]


@dataclass(frozen=True)
class NoticeResponse:
    """Notice metadata and parsed payload exposed by the API."""

    metadata: NoticeMetadata
    payload: dict[str, Any]


__all__ = [
    "NoticeResponse",
    "NoticeMetadata",
    "ConesearchResponse",
    "ConesearchMetadata",
    "ConeRegion",
    "EventSummary",
    "Milestone",
]

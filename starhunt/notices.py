from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from gcn_parser.ep import parse_einstein_probe_wxt
from gcn_parser.fermi import parse_fermi_gbm_alert
from gcn_parser.fermi import parse_fermi_gbm_fin_pos
from gcn_parser.fermi import parse_fermi_gbm_flt_pos
from gcn_parser.fermi import parse_fermi_gbm_gnd_pos
from gcn_parser.svom import is_svom_retraction
from gcn_parser.svom import parse_svom_eclairs
from gcn_parser.svom import parse_svom_grm_trigger
from gcn_parser.svom import parse_svom_mxt
from gcn_parser.svom import parse_svom_retraction
from gcn_parser.svom import SvomRetraction

from starhunt.db import Localization


def _parse_svom_grm_topic(value: bytes):
    if is_svom_retraction(value):
        return parse_svom_retraction(value)
    return parse_svom_grm_trigger(value)


def _parse_svom_eclairs_topic(value: bytes):
    if is_svom_retraction(value):
        return parse_svom_retraction(value)
    return parse_svom_eclairs(value)


def _parse_svom_mxt_topic(value: bytes):
    if is_svom_retraction(value):
        return parse_svom_retraction(value)
    return parse_svom_mxt(value)


@dataclass
class Topic:
    """Kafka topic configuration.

    Attributes:
        topic: Kafka topic name.
        suffix: File suffix used when persisting messages from the topic.
        parser: Callable that parses message bytes into a notice object.
    """

    topic: str
    suffix: str
    parser: Callable


_TOPICS = [
    Topic("gcn.classic.voevent.FERMI_GBM_ALERT", "xml", parse_fermi_gbm_alert),
    Topic("gcn.classic.voevent.FERMI_GBM_FIN_POS", "xml", parse_fermi_gbm_fin_pos),
    Topic("gcn.classic.voevent.FERMI_GBM_FLT_POS", "xml", parse_fermi_gbm_flt_pos),
    Topic("gcn.classic.voevent.FERMI_GBM_GND_POS", "xml", parse_fermi_gbm_gnd_pos),
    Topic("gcn.notices.svom.voevent.eclairs", "xml", _parse_svom_eclairs_topic),
    Topic("gcn.notices.svom.voevent.grm", "xml", _parse_svom_grm_topic),
    Topic("gcn.notices.svom.voevent.mxt", "xml", _parse_svom_mxt_topic),
    Topic("gcn.notices.einstein_probe.wxt.alert", "json", parse_einstein_probe_wxt),
]
_SUFFIXES = {t.topic: t.suffix for t in _TOPICS}
_PARSERS = {t.topic: t.parser for t in _TOPICS}


class UnsupportedTopic(Exception):
    """Notice topic is currently unsupported."""

    def __init__(self, topic: str):
        self.topic = topic
        super().__init__(f"Unsupported message topic: {topic}")


def get_notice_format(topic: str):
    """Return the format of the notice from a topic."""
    try:
        return _SUFFIXES[topic]
    except KeyError as exc:
        raise UnsupportedTopic(topic) from exc


def supported_topics() -> list[str]:
    """Returns a list of supported topics."""
    return [t.topic for t in _TOPICS]


def parse_notice(payload: bytes, topic: str):
    """Parse a notice payload from a supported topic."""
    try:
        parser = _PARSERS[topic]
    except KeyError as exc:
        raise UnsupportedTopic(topic) from exc
    return parser(payload)


@dataclass(frozen=True)
class Notice:
    """A normalized GCN notice."""

    burst_id: str
    localization: Localization | None
    published_at: datetime
    burst_datetime: datetime
    mission: str
    instrument: str
    retractions: tuple[str, ...]


@dataclass(frozen=True)
class NoticeVOEvent(Notice):
    """A normalized VOEvent notice."""

    ivorn: str


@dataclass(frozen=True)
class NoticeJSON(Notice):
    """A normalized JSON notice."""


def normalize_notice(payload: bytes, topic: str) -> NoticeVOEvent | NoticeJSON:
    """
    Normalize a notice for ingestion.

    Args:
        payload: the notice content, e.g. `message.value()`
        topic: the notice topic, e.g. `message.topic()

    Returns:
        a NoticeVOEvent or NoticeJSON, based on the notice format.

    Raises:
        UnsupportedTopic: If the notice topic is not supported.
    """

    def notice_localization(ra: float | None, dec: float | None, err_radius: float | None) -> Localization | None:
        """Normalize parsed notice coordinates into a localization struct."""
        if ra is None or dec is None or err_radius is None or err_radius <= 0:
            return None
        return Localization(ra=ra, dec=dec, err_radius=err_radius)

    parsed_notice = parse_notice(payload, topic)

    match topic:
        case "gcn.classic.voevent.FERMI_GBM_ALERT":
            return NoticeVOEvent(
                ivorn=parsed_notice.ivorn,
                burst_id=str(parsed_notice.trig_id),
                localization=None,
                published_at=parsed_notice.alert_datetime,
                burst_datetime=parsed_notice.burst_datetime,
                mission="Fermi",
                instrument="GBM",
                retractions=(),
            )
        case (
            "gcn.classic.voevent.FERMI_GBM_FIN_POS"
            | "gcn.classic.voevent.FERMI_GBM_FLT_POS"
            | "gcn.classic.voevent.FERMI_GBM_GND_POS"
        ):
            localization = notice_localization(
                parsed_notice.ra,
                parsed_notice.dec,
                parsed_notice.error_radius,
            )
            return NoticeVOEvent(
                ivorn=parsed_notice.ivorn,
                burst_id=str(parsed_notice.trig_id),
                localization=localization,
                published_at=parsed_notice.alert_datetime,
                burst_datetime=parsed_notice.burst_datetime,
                mission="Fermi",
                instrument="GBM",
                retractions=(),
            )
        case "gcn.notices.svom.voevent.eclairs" | "gcn.notices.svom.voevent.grm" | "gcn.notices.svom.voevent.mxt":
            localization = None
            if isinstance(parsed_notice, SvomRetraction):
                retractions = parsed_notice.retractions
            else:
                localization = notice_localization(
                    parsed_notice.ra,
                    parsed_notice.dec,
                    parsed_notice.error_radius,
                )
                retractions = ()

            return NoticeVOEvent(
                ivorn=parsed_notice.ivorn,
                burst_id=parsed_notice.burst_id,
                localization=localization,
                published_at=parsed_notice.alert_datetime,
                burst_datetime=parsed_notice.burst_datetime,
                mission="SVOM",
                instrument=parsed_notice.instrument,
                retractions=retractions,
            )
        case "gcn.notices.einstein_probe.wxt.alert":
            if len(parsed_notice.id) != 1:
                raise ValueError("Einstein Probe WXT notices must have exactly one id")
            localization = notice_localization(
                parsed_notice.ra,
                parsed_notice.dec,
                parsed_notice.ra_dec_error,
            )
            return NoticeJSON(
                burst_id=parsed_notice.id[0],
                localization=localization,
                published_at=parsed_notice.trigger_time,
                burst_datetime=parsed_notice.trigger_time,
                mission="Einstein Probe",
                instrument=parsed_notice.instrument,
                retractions=(),
            )
        case _:
            assert False, "Somehow reached unreachable branch."

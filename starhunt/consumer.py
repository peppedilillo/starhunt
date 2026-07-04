"""Consume GCN notices and schedule follow-up jobs.

- Poll supported Kafka topics.
- Store raw notices on disk and summaries in the database.
- Schedule ZTF/Fink alert conesearch for new events.
"""

from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
import logging
import os
from pathlib import Path
from typing import Callable, Literal

from confluent_kafka import Message
from gcn_kafka import Consumer
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
from psycopg import Connection

from .db import get_event
from .db import init_db_conn
from .db import insert_event
from .db import insert_notice_json
from .db import insert_notice_voevent
from .db import Localization
from .db import mark_retracted_notices
from .utils import is_tz_aware

DEFAULT_CONESEARCH_OFFSET = timedelta(hours=12)
DEFAULT_CONESEARCH_PERIOD = timedelta(hours=6)
DEFAULT_CONESEARCH_TOTAL = 24
DEFAULT_CONESEARCH_MAXRETRY = 3


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


def parse_svom_grm_topic(value: bytes):
    if is_svom_retraction(value):
        return parse_svom_retraction(value)
    return parse_svom_grm_trigger(value)


def parse_svom_eclairs_topic(value: bytes):
    if is_svom_retraction(value):
        return parse_svom_retraction(value)
    return parse_svom_eclairs(value)


def parse_svom_mxt_topic(value: bytes):
    if is_svom_retraction(value):
        return parse_svom_retraction(value)
    return parse_svom_mxt(value)


TOPICS = [
    Topic("gcn.classic.voevent.FERMI_GBM_ALERT", "xml", parse_fermi_gbm_alert),
    Topic("gcn.classic.voevent.FERMI_GBM_FIN_POS", "xml", parse_fermi_gbm_fin_pos),
    Topic("gcn.classic.voevent.FERMI_GBM_FLT_POS", "xml", parse_fermi_gbm_flt_pos),
    Topic("gcn.classic.voevent.FERMI_GBM_GND_POS", "xml", parse_fermi_gbm_gnd_pos),
    Topic("gcn.notices.svom.voevent.eclairs", "xml", parse_svom_eclairs_topic),
    Topic("gcn.notices.svom.voevent.grm", "xml", parse_svom_grm_topic),
    Topic("gcn.notices.svom.voevent.mxt", "xml", parse_svom_mxt_topic),
    Topic("gcn.notices.einstein_probe.wxt.alert", "json", parse_einstein_probe_wxt),
]

SUFFIXES = {t.topic: t.suffix for t in TOPICS}
PARSERS = {t.topic: t.parser for t in TOPICS}
ZTF_CONESEARCH_JOB_TYPE = "ztf_fink_conesearch"
logger = logging.getLogger(__name__)


def init_consumer(
    offset: Literal["earliest", "latest"],
    group_id: str | None,
) -> Consumer:
    """Create the GCN Kafka consumer from environment credentials.

    Args:
        offset: Initial offset policy for partitions without committed offsets.
        group_id: Optional Kafka consumer group identifier.

    Returns:
        A configured GCN Kafka consumer.
    """
    return Consumer(
        client_id=os.environ["GCN_CLIENT_ID"],
        client_secret=os.environ["GCN_CLIENT_SECRET"],
        config={
            "auto.offset.reset": offset,
            "enable.auto.commit": False,
        }
        | (
            {
                "group.id": group_id,
            }
            if group_id is not None
            else {}
        ),
    )


def write_message(
    message: Message,
    outdir: Path,
):
    """Write a Kafka message to disk.

    Args:
        message: Kafka message to persist.
        outdir: Directory where the message file should be written.

    Returns:
        Path to the written message file.
    """
    topic = message.topic()
    filepath = outdir / f"{topic}_{message.partition()}_{message.offset()}.{SUFFIXES[topic]}"
    filepath.write_bytes(message.value())
    return filepath


def notice_localization(ra: float | None, dec: float | None, err_radius: float | None) -> Localization | None:
    """Normalize parsed notice coordinates into a localization struct."""
    if ra is None or dec is None or err_radius is None or err_radius <= 0:
        return None
    return Localization(ra=ra, dec=dec, err_radius=err_radius)


def parse_message(message: Message) -> Notice:
    """Parse a Kafka message into a db-normalized notice shape."""
    topic = message.topic()
    parsed_notice = PARSERS[topic](message.value())

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
        case (
            "gcn.notices.svom.voevent.eclairs"
            | "gcn.notices.svom.voevent.grm"
            | "gcn.notices.svom.voevent.mxt"
        ):
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
            raise ValueError(f"Unsupported message topic: {topic}")


def schedule_ztf_conesearch(
    cursor,
    event_id: int,
    burst_datetime: datetime,
    offset: timedelta,
    period: timedelta,
    total: int,
    max_retry: int,
):
    """Schedule ZTF conesearch jobs for workers to execute.

    The campaign covers ``[burst_datetime, burst_datetime + total * period)``
    with ``total`` windows, each lasting ``period``. Jobs are scheduled after
    each window by ``offset`` to allow broker ingestion and distribution.
    The stable ``scheduled_at`` cutoff and initial ``run_after`` eligibility are
    identical at creation; retries only move ``run_after``.

    Args:
        cursor: Database cursor.
        event_id: Event primary key used by workers to query localization.
        burst_datetime: Campaign start time.
        offset: Delay between each window end and scheduled execution.
        period: Duration of each conesearch window.
        total: Number of conesearch jobs to schedule.
        max_retry: Number of maximum query attempts.

    Raises:
        ValueError: If time bounds or job counts are invalid.
    """
    if not is_tz_aware(burst_datetime):
        raise ValueError("burst_datetime must be timezone-aware")
    if period <= timedelta(0):
        raise ValueError("period must be positive")
    if offset < timedelta(0):
        raise ValueError("offset must be non-negative")
    if total <= 0:
        raise ValueError("total must be positive")
    if max_retry <= 0:
        raise ValueError("max_retry must be positive")

    for index in range(total):
        subject_time_start = burst_datetime + index * period
        subject_time_end = subject_time_start + period
        scheduled_at = subject_time_end + offset
        cursor.execute(
            """
            INSERT INTO jobs (
                event_id,
                job_type,
                subject_time_start,
                subject_time_end,
                scheduled_at,
                run_after,
                max_attempts
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id, job_type, subject_time_start, subject_time_end)
            DO NOTHING
            """,
            (
                event_id,
                ZTF_CONESEARCH_JOB_TYPE,
                subject_time_start,
                subject_time_end,
                scheduled_at,
                scheduled_at,
                max_retry,
            ),
        )


def insert_notice(
    cursor,
    notice: Notice,
    *,
    event_id: int,
    topic: str,
    kafka_partition: int,
    kafka_offset: int,
    raw_uri: str,
) -> int:
    """Insert a normalized notice through the matching storage path."""
    localization = notice.localization
    notice_kwargs = {
        "event_id": event_id,
        "topic": topic,
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
        "mission": notice.mission,
        "instrument": notice.instrument,
        "is_retraction": bool(notice.retractions),
        "published_at": notice.published_at,
        "burst_datetime": notice.burst_datetime,
        "raw_uri": raw_uri,
        "ra": localization.ra if localization is not None else None,
        "dec": localization.dec if localization is not None else None,
        "err_radius": localization.err_radius if localization is not None else None,
    }

    match notice:
        case NoticeVOEvent(ivorn=ivorn):
            return insert_notice_voevent(cursor, ivorn=ivorn, **notice_kwargs)
        case NoticeJSON():
            return insert_notice_json(cursor, **notice_kwargs)
        case _:
            raise TypeError(f"Unsupported normalized notice type: {type(notice).__name__}")


def insert_message(
    message: Message,
    filepath: Path,
    db_conn: Connection,
):
    """Record one Kafka message atomically.

    Args:
        message: Kafka message containing a supported GCN notice.
        filepath: Path where the raw message was persisted.
        db_conn: Database connection used for the transaction.
    """
    topic = message.topic()
    notice = parse_message(message)
    is_retraction = bool(notice.retractions)
    # we annotate the mission for disambiguation: events are not mission-specific
    event_external_id = f"{notice.mission}:{notice.burst_id}"
    raw_uri = filepath.resolve().as_uri()

    try:
        with db_conn.cursor() as cursor:
            # this is acceptable for the moment: ingestion is serial.
            # it may become dangerous if ingestion ever becomes concurrent.
            # two consumer could check for a new event.
            # the first in the race wins and commits its change.
            # the loser in the race gets a UniqueViolation exception from the db
            # and will not insert its message.
            event = get_event(cursor, external_id=event_external_id)
            if event is None:
                event_id = insert_event(cursor, external_id=event_external_id)
                is_new_event = True
            else:
                event_id = event.id
                is_new_event = False

            # we check against retraction because, in general, there is no guarantee
            # that a message on a new event will be actual first alert. we could be
            # starting mid-sequence because we missed alerts, or the actual sequence
            # didn't start with an alert. this means we could be starting a sequence
            # off a retraction.
            if is_new_event and not is_retraction:
                schedule_ztf_conesearch(
                    cursor,
                    event_id=event_id,
                    burst_datetime=notice.burst_datetime,
                    offset=DEFAULT_CONESEARCH_OFFSET,
                    period=DEFAULT_CONESEARCH_PERIOD,
                    total=DEFAULT_CONESEARCH_TOTAL,
                    max_retry=DEFAULT_CONESEARCH_MAXRETRY,
                )
            notice_id = insert_notice(
                cursor,
                notice,
                event_id=event_id,
                topic=topic,
                kafka_partition=message.partition(),
                kafka_offset=message.offset(),
                raw_uri=raw_uri,
            )
            if is_retraction:
                mark_retracted_notices(
                    cursor,
                    event_id=event_id,
                    retraction_notice_id=notice_id,
                    target_ivorns=notice.retractions,
                )

        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise


def main(
    output_directory: Path,
    group_id: str | None = None,
    offset: Literal["earliest", "latest"] = "earliest",
):
    """Consumes GCN notices, record them on disk and db and schedule follow-up jobs.

    Args:
        output_directory: Directory for raw notices.
        group_id: Optional Kafka consumer group ID.
        offset: Initial offset policy when no committed offset exists.
    """
    output_directory.mkdir(parents=True, exist_ok=True)
    consumer = init_consumer(offset, group_id)
    db_conn = init_db_conn()
    try:
        consumer.subscribe([t.topic for t in TOPICS])
        while True:
            for message in consumer.consume(timeout=1):
                if message.error():
                    logger.warning(
                        "Kafka message error",
                        extra={
                            "topic": message.topic(),
                            "partition": message.partition(),
                            "offset": message.offset(),
                            "error": message.error(),
                        },
                    )
                    continue

                filepath = write_message(message=message, outdir=output_directory)
                insert_message(message=message, filepath=filepath, db_conn=db_conn)
                consumer.commit(message=message, asynchronous=False)
                logger.info(
                    "Kafka message committed",
                    extra={
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "notice_path": filepath,
                    },
                )
    finally:
        consumer.close()

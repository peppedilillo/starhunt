from dataclasses import dataclass
from datetime import datetime
import os
from pathlib import Path
from typing import Literal

import click
from confluent_kafka import Message
from gcn_kafka import Consumer
from gcn_parser.fermi import parse_fermi_gbm_alert
from gcn_parser.fermi import parse_fermi_gbm_fin_pos
from gcn_parser.fermi import parse_fermi_gbm_flt_pos
from gcn_parser.fermi import parse_fermi_gbm_gnd_pos
import psycopg
from psycopg import Connection


@dataclass
class Topic:
    topic: str
    suffix: str
    parser: callable


TOPICS = [
    Topic("gcn.classic.voevent.FERMI_GBM_ALERT", "xml", parse_fermi_gbm_alert),
    Topic("gcn.classic.voevent.FERMI_GBM_FIN_POS", "xml", parse_fermi_gbm_fin_pos),
    Topic("gcn.classic.voevent.FERMI_GBM_FLT_POS", "xml", parse_fermi_gbm_flt_pos),
    Topic("gcn.classic.voevent.FERMI_GBM_GND_POS", "xml", parse_fermi_gbm_gnd_pos),
]

SUFFIXES = {t.topic: t.suffix for t in TOPICS}
PARSERS = {t.topic: t.parser for t in TOPICS}


def init_consumer(
    offset: Literal["earliest", "latest"],
    group_id: str | None,
) -> Consumer:
    """Create the GCN Kafka consumer from environment credentials."""
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


def init_db_conn() -> Connection:
    """Create DB connection from environment."""
    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def write_message(
    message: Message,
    outdir: Path,
):
    """Writes a Kafka message to disk."""
    topic = message.topic()
    filepath = outdir / f"{topic}_{message.partition()}_{message.offset()}.{SUFFIXES[topic]}"
    filepath.write_bytes(message.value())
    return filepath


def get_or_create_event(cursor, external_id: str, mission:str, instrument: str) -> int:
    """Return an event id, inserting the event when absent."""
    cursor.execute(
        """
        INSERT INTO events (external_id, mission, instrument)
        VALUES (%s, %s, %s)
        ON CONFLICT (external_id) DO NOTHING
        RETURNING id
        """,
        (external_id, mission, instrument),
    )
    row = cursor.fetchone()
    if row is not None:
        return row[0]

    cursor.execute(
        """
        SELECT id FROM events WHERE external_id = %s
        """,
        (external_id,),
    )
    return cursor.fetchone()[0]


def get_or_create_milestone(
    cursor,
    event_id: int,
    external_id: str,
    subtype: str,
    published_at: datetime,
    subject_time_start: datetime,
    subject_time_end: datetime,
) -> int:
    """Return a milestone id, inserting the milestone when absent."""
    cursor.execute(
        """
        INSERT INTO milestones (
            event_id,
            external_id,
            milestone_type,
            milestone_subtype,
            published_at,
            subject_time_start,
            subject_time_end
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (external_id) DO NOTHING
        RETURNING id
        """,
        (event_id, external_id, "notice", subtype, published_at, subject_time_start, subject_time_end),
    )
    row = cursor.fetchone()
    if row is not None:
        return row[0]

    cursor.execute(
        """
        SELECT id FROM milestones WHERE external_id = %s
        """,
        (external_id,),
    )
    return cursor.fetchone()[0]


def insert_artifact(cursor, milestone_id: int, uri: str):
    """Record an artifact URI for a milestone."""
    cursor.execute(
        """
        INSERT INTO artifacts (milestone_id, artifact_type, uri)
        VALUES (%s, %s, %s)
        ON CONFLICT (milestone_id, artifact_type, uri)
        DO NOTHING
        """,
        (milestone_id, "gcn.voevent", uri),
    )


def insert_message(
    message: Message,
    filepath: Path,
    db_conn: Connection,
):
    """Records message to database."""
    notice = PARSERS[message.topic()](message.value())
    # TODO: mission name is hardcoded here, remember to change this once we add support for other missions
    mission, instrument = "Fermi", "GBM"
    event_external_id = f"{mission}.{instrument}:{notice.trig_id}"
    artifact_uri = filepath.resolve().as_uri()

    try:
        with db_conn.cursor() as cursor:
            event_id = get_or_create_event(
                cursor,
                external_id=event_external_id,
                mission=mission,
                instrument=instrument,
            )
            milestone_id = get_or_create_milestone(
                cursor,
                event_id=event_id,
                external_id=notice.ivorn,
                subtype=message.topic(),
                published_at=notice.alert_datetime,
                subject_time_start=notice.burst_datetime,
                subject_time_end=notice.burst_datetime,
            )
            insert_artifact(
                cursor,
                milestone_id=milestone_id,
                uri=artifact_uri,
            )

        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise


@click.command()
@click.argument(
    "output_directory",
    type=click.Path(path_type=Path, file_okay=False),
)
@click.option("--group-id", default=None, help="Kafka consumer group ID.",)
@click.option(
    "--offset",
    type=click.Choice(["earliest", "latest"]),
    default="earliest",
    show_default=True,
    help="Offset used when no committed offset exists.",
)
def main(
    output_directory: Path,
    group_id: str | None = None,
    offset: Literal["earliest", "latest"] = "earliest",
):
    """Consume GCN notices and store them in OUTPUT_DIRECTORY."""
    output_directory.mkdir(parents=True, exist_ok=True)
    consumer = init_consumer(offset, group_id)
    conn = init_db_conn()
    try:
        consumer.subscribe([t.topic for t in TOPICS])
        while True:
            for message in consumer.consume(timeout=1):
                if message.error():
                    click.echo(message.error(), err=True)
                    continue

                click.echo(f"Received message {message.offset()} over topic {message.topic()}")
                filepath = write_message(message=message, outdir=output_directory)
                click.echo(f"  Wrote message to {filepath}.")
                insert_message(message=message, filepath=filepath, db_conn=conn)
                consumer.commit(message=message, asynchronous=False)
                click.echo(f"  Message committed.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

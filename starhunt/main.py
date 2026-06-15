import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import click
import psycopg
from confluent_kafka import Message
from gcn_kafka import Consumer
from psycopg import Connection


@dataclass
class Topic:
    topic: str
    suffix: str


TOPICS = [
    Topic("gcn.classic.voevent.FERMI_GBM_ALERT", "xml"),
    Topic("gcn.classic.voevent.FERMI_GBM_FIN_POS", "xml"),
    Topic("gcn.classic.voevent.FERMI_GBM_FLT_POS", "xml"),
    Topic("gcn.classic.voevent.FERMI_GBM_GND_POS", "xml"),
]

SUFFIXES = {t.topic: t.suffix for t in TOPICS}


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
        } | ({
            "group.id": group_id,
        } if group_id is not None else {}),
    )


def init_conn() -> Connection:
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


@click.command()
@click.argument(
    "output_directory",
    type=click.Path(path_type=Path, file_okay=False),
)
@click.option("--group-id", default=None, help="Kafka consumer group ID.")
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

                consumer.commit(message=message, asynchronous=False)
                click.echo(f"  Message committed.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

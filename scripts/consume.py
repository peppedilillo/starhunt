from pathlib import Path
from typing import Literal

import click

from starhunt.consumer import init_consumer, init_db_conn, TOPICS, write_message, insert_message


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

from pathlib import Path
from typing import Literal

import click

from starhunt.consumer import main as run_consumer_main
from starhunt.logging import configure_logging


@click.command()
@click.argument(
    "output_directory",
    type=click.Path(path_type=Path, file_okay=False),
)
@click.option(
    "--group-id",
    default=None,
    help="Kafka consumer group ID.",
)
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
    configure_logging("consumer")
    run_consumer_main(
        output_directory=output_directory,
        group_id=group_id,
        offset=offset,
    )

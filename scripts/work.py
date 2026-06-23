from pathlib import Path

import click

from starhunt.worker import main as run_worker_main


@click.command()
@click.argument(
    "output_directory",
    type=click.Path(path_type=Path, file_okay=False),
)
@click.option(
    "--worker-id",
    default=None,
    help="Worker identifier. A UUID is generated when omitted.",
)
def main(
    output_directory: Path,
    worker_id: str | None = None,
):
    """Execute scheduled Starhunt jobs and store outputs in OUTPUT_DIRECTORY."""
    run_worker_main(output_directory=output_directory, worker_id=worker_id)

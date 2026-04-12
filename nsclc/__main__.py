"""Entry point for `python -m nsclc`.

Subcommands are registered as click commands on the `cli` group.
"""

from __future__ import annotations

import click

from nsclc.build_subset import main as build_subset_cmd
from nsclc.workflows import main as workflows_cmd


@click.group()
def cli() -> None:
    """NSCLC Evidence Radar — deterministic pipeline."""


cli.add_command(build_subset_cmd)
cli.add_command(workflows_cmd)


if __name__ == "__main__":
    cli()

"""Cli modules for ot-orchestration."""

import click
import logging
from ot_orchestration.cli.fetch_raw_sumstat_paths import fetch_raw_sumstat_paths
from ot_orchestration.cli.generate_dotenv import generate_dotenv
from ot_orchestration.cli.harmonise_sumstats import harmonise_sumstats

logging.basicConfig(level=logging.INFO)
asci_art = """
   ____  ______   ____            __              __             __  _
  / __ \/_  __/  / __ \__________/ /_  ___  _____/ /__________ _/ /_(_)___  ____
 / / / / / /    / / / / ___/ ___/ __ \/ _ \/ ___/ __/ ___/ __ `/ __/ / __ \/ __ \\
/ /_/ / / /    / /_/ / /  / /__/ / / /  __(__  ) /_/ /  / /_/ / /_/ / /_/ / / / /
\____/ /_/     \____/_/   \___/_/ /_/\___/____/\__/_/   \__,_/\__/_/\____/_/ /_/
"""


@click.group()
def cli():
    """Orchestration around Open Targets pipelines."""
    print(asci_art)


if __name__ == "__main__":
    cli.add_command(fetch_raw_sumstat_paths)
    cli.add_command(generate_dotenv)
    cli.add_command(harmonise_sumstats)
    cli()


__all__ = ["cli"]

"""Utility scripts."""
import click

from ot_orchestration.scripts import generate_dotenv


@click.group()
def ot():
    """Open Targets Orchestration Command Line Interface."""


ot.add_command(generate_dotenv.main, name = "generate_dotenv")

"""Generate dotenv file for airflow configuration."""

import logging
import os
import sys
from pathlib import Path

import click

logging.basicConfig(level=logging.INFO)
__version__ = "1.0.0"


@click.command()
def generate_dotenv():
    """Ïnteractive script to generate environment variables for local airflow stack."""
    env_file = ".env"
    logging.info("Checking if %s file exists", env_file)
    if Path(env_file).exists():
        logging.info(".env file already exists, exiting")
        sys.exit(0)
    gcp_service_account = click.prompt(
        "Provide path to the gcp service account",
        type=click.Path(exists=True, file_okay=True, dir_okay=False),
    )
    logging.info(f"{gcp_service_account=}")
    uid = os.getuid()
    logging.info(f"{uid=}")
    gcp_project_id = click.prompt("Provide gcp project id")

    mapping = {
        "AIRFLOW_UID": uid,
        "GOOGLE_APPLICATION_CREDENTIAL": gcp_service_account,
        "GCP_PROJECT_ID": gcp_project_id,
    }

    with open(env_file, "w") as f:
        for key, val in mapping.items():
            f.write(f"{key}={val}")


__all__ = ["generate_dotenv"]

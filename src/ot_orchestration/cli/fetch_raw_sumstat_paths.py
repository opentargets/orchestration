"""Fetching raw sumstat paths for the manifest."""

import click
import logging
from google.cloud import storage
import pandas as pd


@click.command()
@click.option(
    "--manifest",
    "-m",
    help="one columnar csv file with GWAS Catalog study id, no header",
    type=click.Path(file_okay=True, dir_okay=False, exists=True),
)
@click.option(
    "--output",
    "-o",
    help="two columnar csv file with GWAS Catalog study id and raw sumstat path, no header",
    type=click.Path(file_okay=True, dir_okay=False, exists=False),
)
@click.option(
    "--bucket-name",
    "-b",
    help="bucket name where raw studies are found",
    type=click.STRING,
    default="gwas_catalog_data",
)
def fetch_raw_sumstat_paths(manifest: str, output: str, bucket_name: str) -> None:
    """Lists all the blobs in the bucket."""
    manifest_df = pd.read_csv(manifest, header=None)
    logging.info(
        "Using following arguments, manifest: %s, output: %s, bucket_name: %s",
        manifest,
        output,
        bucket_name,
    )
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(
        bucket_name, prefix="raw_summary_statistics", match_glob="**/*h.tsv.gz"
    )

    raw_study_state = [
        {"filename": f.name, "studyId": f.name.split("/")[2]} for f in blobs
    ]
    raw_study_df = pd.DataFrame.from_records(raw_study_state)
    manifest_df.columns = ["studyId"]
    result = manifest_df.merge(raw_study_df, how="inner", on=["studyId"])

    logging.info(f"Found {raw_study_df.shape[0]} raw sumstats")
    logging.info(f"Found {manifest_df.shape[0]} studyIds im provided manifest")
    logging.info(f"Found matches for {result.shape[0]}/{manifest_df.shape[0]} records.")
    result.to_csv(output, index=False)

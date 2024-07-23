"""Process GWAS Catalog summary statistics in batch job."""

import os
from ot_orchestration.utils import GCSIOManager
from ot_orchestration.types import Manifest_Object
import logging
import subprocess
import click


def harmonise(manifest: Manifest_Object) -> Manifest_Object:
    """Run Harmonisation."""
    raw_path = manifest["rawPath"]
    harmonised_path = manifest["harmonisedPath"]
    study_id = manifest["studyId"]
    command = [
        "poetry",
        "run",
        "gentropy",
        "step=gwas_catalog_sumstat_preprocess",
        f'step.raw_sumstats_path="{raw_path}"',
        f'step.out_sumstats_path="{harmonised_path}"',
        "'+step.session.extended_spark_conf={spark.jars:https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar}'",
        "'+step.session.extended_spark_conf={spark.dynamicAllocation.enabled:False}'",
        "'+step.session.extended_spark_conf={spark.driver.memory:30g}'",
        "'+step.session.extended_spark_conf={spark.kryoserializer.buffer.max:500m}'",
        "'+step.session.extended_spark_conf={spark.driver.maxResultSize:5g}'",
    ]
    result = subprocess.run(args=command, capture_output=True)
    if result.returncode != 0:
        logging.error("Harmonisation for study %s failed!", study_id)
        logging.error(result.stderr)
        manifest["passHarmonisation"] = False
        GCSIOManager().dump(manifest["manifestPath"], manifest)
        exit(1)
    else:
        logging.info("Harmonisation for study %s completed successfully!", study_id)
        manifest["passHarmonisation"] = True
    return manifest


def qc(manifest: Manifest_Object) -> Manifest_Object:
    """Run QC."""
    return manifest


def clumping(manifest: Manifest_Object) -> Manifest_Object:
    """Run Clumping."""
    return manifest


@click.command()
def gwas_catalog_process_in_batch():
    """Run gwas catalog processing of summary statistics in batch. This includes harmonisation, QC and clumping."""
    PROCESSING_ORDER = ["harmonisation"]
    MANIFEST_PATH = os.environ.get("MANIFEST_PATH")
    if MANIFEST_PATH is None:
        logging.error("MANIFEST_PATH not set!")
        exit(1)

    manifest = GCSIOManager().load(MANIFEST_PATH)
    study = manifest["studyId"]
    PROCESSING_STEPS = {"harmonisation": harmonise, "qc": qc, "clumping": clumping}
    for step in PROCESSING_ORDER:
        if manifest[f"pass{step.capitalize()}"]:
            logging.info("Skipping %s", step)
            continue
        logging.info("Running %s for %s", step, study)
        manifest = PROCESSING_STEPS[step](manifest)
        logging.info("Finished %s for %s", step, study)

    GCSIOManager().dump(MANIFEST_PATH, manifest)


__all__ = ["gwas_catalog_process_in_batch"]

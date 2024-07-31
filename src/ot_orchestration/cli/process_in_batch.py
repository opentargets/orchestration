"""Process GWAS Catalog summary statistics in batch job."""

from ot_orchestration.utils import IOManager, GWASCatalogPipelineManifest
import logging
import subprocess
import click
import os
import sys

MANIFEST_PATH_ENV_VAR = "MANIFEST_PATH"


def harmonise_step(
    manifest: GWASCatalogPipelineManifest,
) -> GWASCatalogPipelineManifest:
    """Run Harmonisation."""
    raw_path = manifest["rawPath"]
    harmonised_path = manifest["harmonisedPath"]
    study_id = manifest["studyId"]
    manifest_path = manifest["manifestPath"]
    pass_harmonisation = manifest["passHarmonisation"]
    logging.info("Running %s for %s", "harmonisation", study_id)

    command = [
        "gentropy",
        "step=gwas_catalog_sumstat_preprocess",
        f'step.raw_sumstats_path="{raw_path}"',
        f'step.out_sumstats_path="{harmonised_path}"',
        "+step.session.extended_spark_conf={spark.jars:'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar'}",
        "+step.session.extended_spark_conf={spark.dynamicAllocation.enabled:'false'}",
        "+step.session.extended_spark_conf={spark.driver.memory:'30g'}",
        "+step.session.extended_spark_conf={spark.kryoserializer.buffer.max:'500m'}",
        "+step.session.extended_spark_conf={spark.driver.maxResultSize:'5g'}",
    ]
    if IOManager().resolve(harmonised_path).exists():
        if not pass_harmonisation:
            logging.info("Harmonisation result exists for %s. Skipping", study_id)
            manifest["passHarmonisation"] = True
        return manifest

    logging.info("Running command %s", " ".join(command))
    command = ["echo", "RUNNING!"]
    result = subprocess.run(args=command, capture_output=True)
    logging.info(result)
    if result.returncode != 0:
        logging.error("Harmonisation for study %s failed!", study_id)
        error_msg = result.stderr.decode()
        logging.error(error_msg)
        manifest["passHarmonisation"] = False
        logging.info("Dumping manifest to %s", manifest_path)
        IOManager().resolve(manifest_path).dump(manifest)
        sys.exit(1)

    logging.info("Harmonisation for study %s succeded!", study_id)
    manifest["passHarmonisation"] = True
    return manifest


def qc_step(manifest: GWASCatalogPipelineManifest) -> GWASCatalogPipelineManifest:
    """Run QC."""
    harmonised_path = manifest["harmonisedPath"]
    qc_path = manifest["qcPath"]
    study_id = manifest["studyId"]
    manifest_path = manifest["manifestPath"]

    command = [
        "gentropy",
        "step=summary_statistics_qc",
        f'step.gwas_path="{harmonised_path}"',
        f'step.output_path="{qc_path}"',
        f'step.study_id="{study_id}"',
        "+step.session.extended_spark_conf={spark.jars:'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar'}",
        "+step.session.extended_spark_conf={spark.dynamicAllocation.enabled:'false'}",
        "+step.session.extended_spark_conf={spark.driver.memory:'30g'}",
        "+step.session.extended_spark_conf={spark.kryoserializer.buffer.max:'500m'}",
        "+step.session.extended_spark_conf={spark.driver.maxResultSize:'5g'}",
    ]
    result_exists = IOManager().resolve(qc_path).exists()
    if result_exists:
        logging.info("QC result exists for %s. Skipping", study_id)
        manifest["passQC"] = True
        return manifest

    result = subprocess.run(args=command, capture_output=True)
    if result.returncode != 0:
        logging.error("QC for study %s failed!", study_id)
        error_msg = result.stderr.decode()
        logging.error(error_msg)
        manifest["passQC"] = False
        logging.info("Dumping manifest to %s", manifest_path)
        IOManager().resolve(manifest_path).dump(manifest)
        exit(1)

    logging.info("QC for study %s succeded!", study_id)
    manifest["passQC"] = True
    return manifest


def qc_consolidation_step(
    manifest: GWASCatalogPipelineManifest,
) -> GWASCatalogPipelineManifest:
    """Check if sumstats pass qc thresholds."""
    return manifest


def clump_step(manifest: GWASCatalogPipelineManifest) -> GWASCatalogPipelineManifest:
    """Run Clumping."""
    return manifest


@click.command()
def gwas_catalog_pipeline():
    """Run gwas catalog processing of summary statistics in batch.

    This includes harmonisation, QC and clumping.
    This command requires setting the `MANIFEST_PATH` in the
    environment. The variable should be the reference to the path with the
    manifest file.
    """
    logging.debug("Reading MANIFEST_PATH env variable")
    manifest_path = os.getenv(MANIFEST_PATH_ENV_VAR)
    if not manifest_path:
        logging.error("MANIFEST_PATH environment variable is missing")
        sys.exit(1)
    logging.debug("MANIFEST_PATH: %s", manifest_path)
    manifest = GWASCatalogPipelineManifest.from_file(manifest_path)
    logging.debug("MANIFEST: %s", manifest)
    # for now dummy implementatin of the pipeline processing order
    manifest = harmonise_step(manifest)
    manifest = qc_step(manifest)
    manifest = qc_consolidation_step(manifest)
    manifest = clump_step(manifest)

    IOManager().resolve(manifest_path).dump(manifest)


__all__ = ["gwas_catalog_pipeline"]

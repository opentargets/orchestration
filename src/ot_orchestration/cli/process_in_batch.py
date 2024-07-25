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
        "+step.session.extended_spark_conf={spark.jars:'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar'}",
        "+step.session.extended_spark_conf={spark.dynamicAllocation.enabled:'false'}",
        "+step.session.extended_spark_conf={spark.driver.memory:'30g'}",
        "+step.session.extended_spark_conf={spark.kryoserializer.buffer.max:'500m'}",
        "+step.session.extended_spark_conf={spark.driver.maxResultSize:'5g'}",
    ]
    if GCSIOManager().exists(harmonised_path):
        logging.info("Harmonisation result exists for %s. Skipping", study_id)
        manifest["passHarmonisation"] = True
        return manifest

    result = subprocess.run(args=command, capture_output=True)
    if result.returncode != 0:
        logging.error("Harmonisation for study %s failed!", study_id)
        error_msg = result.stderr.decode()
        logging.error(error_msg)
        manifest["passHarmonisation"] = False
        logging.info("Dumping manifest to %s", manifest["manifestPath"])
        GCSIOManager().dump(manifest["manifestPath"], manifest)
        exit(1)

    logging.info("Harmonisation for study %s succeded!", study_id)
    manifest["passHarmonisation"] = True
    return manifest


def qc(manifest: Manifest_Object) -> Manifest_Object:
    """Run QC."""
    harmonised_path = manifest["harmonisedPath"]
    qc_path = manifest["qcPath"]
    study_id = manifest["studyId"]
    command = [
        "poetry",
        "run",
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
    result_exists = GCSIOManager().exists(qc_path)
    logging.info("Result exists: %s", result_exists)
    if GCSIOManager().exists(qc_path):
        logging.info("QC result exists for %s. Skipping", study_id)
        manifest["passQC"] = True
        return manifest

    result = subprocess.run(args=command, capture_output=True)
    if result.returncode != 0:
        logging.error("QC for study %s failed!", study_id)
        error_msg = result.stderr.decode()
        logging.error(error_msg)
        manifest["passQC"] = False
        logging.info("Dumping manifest to %s", manifest["manifestPath"])
        GCSIOManager().dump(manifest["manifestPath"], manifest)
        exit(1)

    logging.info("QC for study %s succeded!", study_id)
    manifest["passQC"] = True
    return manifest


def qc_consolidation(manifest: Manifest_Object) -> Manifest_Object:
    pass


def clumping(manifest: Manifest_Object) -> Manifest_Object:
    """Run Clumping."""
    harmonised_path = manifest["harmonisedPath"]
    clumping_path = manifest["clumpingPath"]
    study_id = manifest["studyId"]
    command = [
        "poetry",
        "run",
        "gentropy",
        "step=clumping",
        f'step.gwas_path="{harmonised_path}"',
        f'step.output_path="{clumping_path}"',
        f'step.study_id="{study_id}"',
        "+step.session.extended_spark_conf={spark.jars:'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar'}",
        "+step.session.extended_spark_conf={spark.dynamicAllocation.enabled:'false'}",
        "+step.session.extended_spark_conf={spark.driver.memory:'30g'}",
        "+step.session.extended_spark_conf={spark.kryoserializer.buffer.max:'500m'}",
        "+step.session.extended_spark_conf={spark.driver.maxResultSize:'5g'}",
    ]
    if GCSIOManager().exists(clumping_path):
        logging.info("Clumping result exists for %s. Skipping", study_id)
        manifest["passClumping"] = True
        return manifest

    result = subprocess.run(args=command, capture_output=True)
    if result.returncode != 0:
        logging.error("Clumping for study %s failed!", study_id)
        error_msg = result.stderr.decode()
        logging.error(error_msg)
        manifest["passClumping"] = False
        logging.info("Dumping manifest to %s", manifest["manifestPath"])
        GCSIOManager().dump(manifest["manifestPath"], manifest)
        exit(1)
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

"""Airflow DAG for GWAS Catalog sumstat harmonisation."""

from __future__ import annotations

import logging
from pathlib import Path

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG

from ot_orchestration.operators.batch.harmonisation import (
    BatchIndexOperator,
    GeneticsBatchJobOperator,
)
from ot_orchestration.utils import (
    find_node_in_config,
    read_yaml_config,
)
from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs

SOURCE_CONFIG_FILE_PATH = (
    Path(__file__).parent / "config" / "gwas_catalog_harmonisation.yaml"
)
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH)


@task(task_id="begin")
def begin():
    """Starting the DAG execution."""
    logging.info("STARTING")
    logging.info(config)


@task(task_id="end")
def end():
    """Finish the DAG execution."""
    logging.info("FINISHED")


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog Sumstat Harmonisation",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
):
    node_config = find_node_in_config(config["nodes"], "generate_sumstat_index")
    batch_index = BatchIndexOperator(
        task_id=node_config["id"],
        batch_index_specs=node_config["google_batch_index_specs"],
    )
    node_config = find_node_in_config(config["nodes"], "gwas_catalog_harmonisation")
    harmonisation_batch_job = GeneticsBatchJobOperator.partial(
        task_id=node_config["id"],
        job_name="harmonisation",
        google_batch=node_config["google_batch"],
    ).expand(batch_index_row=batch_index.output)

    chain(
        begin(),
        batch_index,
        # harmonisation_batch_job,
        end(),
    )

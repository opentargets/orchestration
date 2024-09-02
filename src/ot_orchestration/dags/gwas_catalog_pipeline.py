"""Airflow DAG for GWAS Catalog processing."""

from __future__ import annotations

import logging
from pathlib import Path

from airflow.models.dag import DAG

from ot_orchestration.dynamic_dags import generic_genetics_dag
from ot_orchestration.utils import read_yaml_config
from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs

SOURCE_CONFIG_FILE_PATH = (
    Path(__file__).parent / "config" / "gwas_catalog_pipeline.yaml"
)
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH)
logging.basicConfig(level=logging.INFO)


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics - Finngen ETL",
    params=config,
    default_args=shared_dag_args,
    **shared_dag_kwargs,
) as dag:
    generic_genetics_dag()

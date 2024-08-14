"""Airflow DAG for GWAS Catalog processing."""

from __future__ import annotations

from ot_orchestration.utils import read_yaml_config
from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs
from airflow.models.dag import DAG
from pathlib import Path

import logging
from ot_orchestration.dynamic_dags import generic_genetics_dag


config_path = "/opt/airflow/config/config.yaml"
config = read_yaml_config(config_path)
logging.basicConfig(level=logging.INFO)


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics - Finngen ETL",
    params=config,
    default_args=shared_dag_args,
    **shared_dag_kwargs,
) as dag:
    generic_genetics_dag()

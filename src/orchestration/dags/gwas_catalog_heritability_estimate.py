"""Airflow DAG for running gentropy heritability estimation.

This DAG wraps the heritability estimation step defined in
``src/orchestration/dags/config/gentropy_heritability_estimate.yaml``.  It is
intended to be run on demand when new studies need heritability estimates.

The DAG loads the YAML configuration, resolves any templated environment
variables, constructs a batch index manifest via ``BatchIndexOperator``, and
submits a separate Google Batch job for each manifest row using
``BatchJobOperator``.  Only studies without existing heritability
estimates will be scheduled for processing, as determined by the custom
manifest generator defined in
``orchestration.operators.batch.manifest_generators.heritability_estimate``.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain

from orchestration.operators.batch.generic import BatchIndexOperator, BatchJobOperator
from orchestration.utils import find_environment_vars, find_node_in_config, read_yaml_config

# Default DAG arguments.  The schedule interval is left as ``None`` to
# indicate that this DAG should only be triggered manually when needed.
default_args = {
    "owner": "opentargets",
}

with DAG(
    dag_id="gentropy_heritability_estimate",
    description="Run heritability estimation for harmonised summary statistics using gentropy",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["gentropy", "heritability"],
) as dag:
    # Load the YAML configuration.  The string corresponds to the Python
    # import path for the config file without the `.yaml` suffix.
    config = read_yaml_config("orchestration.dags.config.gentropy_heritability_estimate")
    # Extract any templated variables from the Airflow context (e.g. ``release_uri``)
    # so that the correct node in the config is selected.
    environment_vars = find_environment_vars(config)
    node = find_node_in_config(config, environment_vars)

    # This DAG contains only a single step: heritability_estimate.  We pull
    # the corresponding batch specifications from the config.
    step_name = "heritability_estimate"
    step_config = node["steps"][step_name]

    # Create a BatchIndexOperator which builds the manifest of tasks to run.
    batch_index = BatchIndexOperator(
        task_id=f"{step_name}.batch_index",
        batch_index_specs=step_config["google_batch_index_specs"],
    )

    # Create a BatchJobOperator for each task in the manifest using Airflow's
    # dynamic task mapping.  ``partial`` is used to freeze the static
    # parameters, while ``expand`` maps over the output of the index.
    batch_jobs = BatchJobOperator.partial(
        task_id=f"{step_name}.batch_job",
        google_batch=step_config["google_batch"],
    ).expand(batch_index_row=batch_index.output)

    # Chain the tasks to enforce ordering: the index must complete before any
    # batch jobs are submitted.
    chain(batch_index, batch_jobs)  # batch jobs are submitted.
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.models.baseoperator import chain

from orchestration.operators.batch.generic import BatchIndexOperator, BatchJobOperator
from orchestration.utils import read_yaml_config, resource_name

_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


def format_config(obj: Any, vars_dict: dict[str, str]) -> Any:
    """Recursively format strings in a config structure.

    Only placeholders that exactly match keys in ``vars_dict`` are replaced,
    for example ``{study_index_path}``.

    Strings that contain Hydra-style dict syntax, such as
    ``{spark.jars:...}``, are left untouched.
    """
    if isinstance(obj, str):
        return _PLACEHOLDER_RE.sub(
            lambda match: vars_dict.get(match.group(1), match.group(0)),
            obj,
        )
    if isinstance(obj, list):
        return [format_config(x, vars_dict) for x in obj]
    if isinstance(obj, dict):
        return {k: format_config(v, vars_dict) for k, v in obj.items()}
    return obj


default_args = {
    "owner": "opentargets",
}

with DAG(
    dag_id="gentropy_heritability_estimate",
    description="Run heritabilbatch_jobsity estimation for harmonised summary statistics using gentropy",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["gentropy", "heritability"],
) as dag:
    config = read_yaml_config(Path(__file__).parent / "config" / "gwas_catalog_heritability_estimate.yaml")

    env_name = config["env"]
    env_spec = next(env for env in config["environment_specs"] if env["name"] == env_name)
    env_vars = env_spec["vars"]

    step_name = "heritability_estimate"
    node = next(node for node in config["nodes"] if node["id"] == step_name)
    step_config = format_config(node, env_vars)

    batch_index = BatchIndexOperator(
        task_id=f"{step_name}.batch_index",
        batch_index_specs=step_config["google_batch_index_specs"],
    )

    batch_jobs = BatchJobOperator.partial(
        task_id=f"{step_name}.batch_job",
        job_name=f"up-{step_name.replace('_', '-')}",
        google_batch=step_config["google_batch"],
    ).expand(batch_index_row=batch_index.output)

    chain(batch_index, batch_jobs)

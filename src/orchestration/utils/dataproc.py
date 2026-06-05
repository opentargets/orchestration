"""Airflow boilerplate code that interfaces with Dataproc operators which can be shared by several DAGs."""

from __future__ import annotations

import logging
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.utils.trigger_rule import TriggerRule

from orchestration.operators.dataproc import (
    CreateClusterOperator,
    CustomClusterConfig,
    DeleteClusterOperator,
    GentropyJobBuilder,
    SubmitJobOperator,
)
from orchestration.utils.common import (
    GCP_PROJECT_GENETICS,
    GCP_REGION,
)
from orchestration.utils.labels import Labels

log: logging.Logger = logging.getLogger(__name__)
# TODO: Delete all these once they are no longer used here.
GCP_DATAPROC_IMAGE = "2.2"
GCP_AUTOSCALING_POLICY = "otg-etl"
GCP_EFM_AUTOSCALING_POLICY = "otg-efm"
GENTROPY_CLI_SCRIPT = "gs://genetics_etl_python_playground/initialisation/cli.py"
GENTROPY_CLUSTER_INIT_SCRIPT = "gs://genetics_etl_python_playground/initialisation/install_dependencies_on_cluster.sh"


def create_cluster(cluster_name: str, cluster_config: CustomClusterConfig) -> CreateClusterOperator:
    """Wrapper over `CreateClusterOperator` tailored for staging dataproc clusters."""
    return CreateClusterOperator(
        project_id=GCP_PROJECT_GENETICS,
        region=GCP_REGION,
        task_id=f"create_cluster_{cluster_name}",
        cluster_name=cluster_name,
        cluster_config=cluster_config,
        labels=Labels(project=GCP_PROJECT_GENETICS),
    )


def delete_cluster(cluster_name: str) -> DeleteClusterOperator:
    """Generate an Airflow task to delete a Dataproc cluster.

    Args:
        cluster_name (str): Name of the cluster.

    Returns:
        DeleteClusterOperator: Airflow task to delete a Dataproc cluster.
    """
    return DeleteClusterOperator(
        task_id=f"delete_cluster_{cluster_name}",
        project_id=GCP_PROJECT_GENETICS,
        cluster_name=cluster_name,
        region=GCP_REGION,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )


def submit_gentropy_step(
    cluster_name: str,
    step_name: str,
    params: dict[str, Any] | None = None,
) -> SubmitJobOperator:
    """Submit a PySpark job from a gentropy step to execute a specific CLI step.

    Args:
        cluster_name (str): Name of the cluster to submit the job to.
        step_name (str): Name of the step to execute, should be in format `step: 'step_name'`.
        params (dict[str, Any] | None): Parameters to pass to the gentropy step.
            The keys should be in format `*step*:key`.


    Returns:
        DataprocSubmitJobOperator: Airflow task to submit a PySpark job to execute a specific CLI step.

    First parameter should represent the gentropy step to run and be in format `step: 'step_name'`.
    The rest of parameter values can be of any non non nested data type (int, float, str, bool, None, list, dict).
    The values can not be nested data types, like dict of lists. In that context, one should refer to
    https://hydra.cc/docs/advanced/override_grammar/basic/. The key has to be passed in a format `*step*:key`

    Complex examples:
    * step: "validate_study"
    * step.session.write_mode: "overwrite"
    * +step.session.extended_spark_conf: "{spark.jars:https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar}"
    """
    labels = Labels(project=GCP_PROJECT_GENETICS, extra={"step": step_name})
    log.info(f"Sending {step_name} to {cluster_name} with {params}")

    job = GentropyJobBuilder(
        main_python_file_uri=GENTROPY_CLI_SCRIPT,
        params=params,
        properties={
            "spark.jars": "/opt/conda/miniconda3/lib/python3.11/site-packages/hail/backend/hail-all-spark.jar",
            "spark.driver.extraClassPath": "/opt/conda/miniconda3/lib/python3.11/site-packages/hail/backend/hail-all-spark.jar",
            "spark.executor.extraClassPath": "./hail-all-spark.jar",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.kryo.registrator": "is.hail.kryo.HailKryoRegistrator",
        },
    ).build()

    return SubmitJobOperator(
        task_id=step_name,
        project_id=GCP_PROJECT_GENETICS,
        region=GCP_REGION,
        cluster_name=cluster_name,
        step_name=step_name,
        py_spark_job=job,
        labels=labels,
    )


def generate_dataproc_task_chain(
    tasks: list[BaseOperator],
    *,
    create: bool = True,
    delete: bool = True,
    **kwargs: Any,
) -> list[BaseOperator]:
    """For a list of Dataproc tasks, generate a complete chain of tasks.

    This function adds `create_cluster` and `delete_cluster` tasks upstream (create) and downstream (delete)

    Args:
        tasks (list[BaseOperator]): List of tasks to execute.
        create (bool): Indicate if the cluster should be created prior the first dataproc task. Defaults to True.
        delete (bool): Indicate if the cluster should be deleted after the last dataproc task. Defaults to True.
        **kwargs (Any): keyword arguments passed to the `create_cluster`. Should always contain the cluster_name.

    Returns:
        list[BaseOperator]: list of input tasks with muted chain.
    """
    if create:
        create_cluster_task = create_cluster(
            cluster_name=kwargs["cluster_name"],
            cluster_config=CustomClusterConfig(**kwargs["cluster_config"]),
        )
        for task in tasks:
            if not task.get_direct_relatives(upstream=True):
                task.set_upstream(create_cluster_task)
    if delete:
        delete_cluster_task = delete_cluster(cluster_name=kwargs["cluster_name"])
        for task in tasks:
            if not task.get_direct_relatives(upstream=False):
                task.set_downstream(delete_cluster_task)

    return tasks

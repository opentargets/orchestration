"""Airflow boilerplate code that interfaces with Dataproc operators which can be shared by several DAGs."""

from __future__ import annotations

from typing import Any

from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from ot_orchestration.utils import convert_params_to_hydra_positional_arg

# from ot_orchestration.utils import GCSPath
from ot_orchestration.utils.common import (
    GCP_AUTOSCALING_POLICY,
    GCP_DATAPROC_IMAGE,
    GCP_PROJECT_GENETICS,
    GCP_REGION,
    GCP_ZONE,
)
from ot_orchestration.utils.path import GCSPath


def create_cluster(
    cluster_name: str,
    master_machine_type: str = "n1-highmem-16",
    worker_machine_type: str = "n1-standard-16",
    num_workers: int = 2,
    num_preemptible_workers: int = 0,
    num_local_ssds: int = 1,
    autoscaling_policy: str = GCP_AUTOSCALING_POLICY,
    master_disk_size: int = 500,
    cluster_init_script: str | None = None,
    cluster_metadata: dict[str, str] | None = None,
) -> DataprocCreateClusterOperator:
    """Generate an Airflow task to create a Dataproc cluster. Common parameters are reused, and varying parameters can be specified as needed.

    Args:
        cluster_name (str): Name of the cluster.
        master_machine_type (str): Machine type for the master node. Defaults to "n1-highmem-8".
        worker_machine_type (str): Machine type for the worker nodes. Defaults to "n1-standard-16".
        num_workers (int): Number of worker nodes. Defaults to 2.
        num_preemptible_workers (int): Number of preemptible worker nodes. Defaults to 0.
        num_local_ssds (int): How many local SSDs to attach to each worker node, both primary and secondary. Defaults to 1.
        autoscaling_policy (str): Name of the autoscaling policy to use. Defaults to GCP_AUTOSCALING_POLICY.
        master_disk_size (int): Size of the master node's boot disk in GB. Defaults to 500.
        cluster_init_script (str | None): Cluster initialization scripts.
        cluster_metadata (str | None): Cluster METADATA.

    Returns:
        DataprocCreateClusterOperator: Airflow task to create a Dataproc cluster.
    """
    # Create base cluster configuration.
    cluster_config = ClusterGenerator(
        project_id=GCP_PROJECT_GENETICS,
        zone=GCP_ZONE,
        master_machine_type=master_machine_type,
        worker_machine_type=worker_machine_type,
        master_disk_size=master_disk_size,
        worker_disk_size=500,
        num_preemptible_workers=num_preemptible_workers,
        num_workers=num_workers,
        image_version=GCP_DATAPROC_IMAGE,
        enable_component_gateway=True,
        metadata=cluster_metadata,
        # idle_delete_ttl=30 * 60,  # In seconds.
        init_actions_uris=[cluster_init_script] if cluster_init_script else None,
        autoscaling_policy=f"projects/{GCP_PROJECT_GENETICS}/regions/{GCP_REGION}/autoscalingPolicies/{autoscaling_policy}",
    ).make()

    # If specified, amend the configuration to include local SSDs for worker nodes.
    if num_local_ssds:
        for worker_section in ("worker_config", "secondary_worker_config"):
            # Create a disk config section if it does not exist.
            cluster_config[worker_section].setdefault("disk_config", {})
            # Specify the number of local SSDs.
            cluster_config[worker_section]["disk_config"]["num_local_ssds"] = (
                num_local_ssds
            )

    # Return the cluster creation operator.
    return DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=GCP_PROJECT_GENETICS,
        cluster_config=cluster_config,
        region=GCP_REGION,
        cluster_name=cluster_name,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )


def submit_gentropy_step(
    cluster_name: str,
    step_name: str,
    python_main_module: str,
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
    params: dict[str, Any] | None = None,
) -> DataprocSubmitJobOperator:
    """Submit a PySpark job from a gentropy step to execute a specific CLI step.

    Args:
        cluster_name (str): Name of the cluster.
        step_name (str): Name of the gentropy step to run.
        trigger_rule (TriggerRule): Trigger rule for the task. Defaults to TriggerRule.ALL_SUCCESS.
        python_main_module (str): GCS path to the gentropy CLI wrapper script.
        params (list[str]): Optional parameters to append to the gentropy step, must be in key:value.

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
    return submit_pyspark_job(
        cluster_name=cluster_name,
        task_id=step_name,
        python_main_module=python_main_module,
        trigger_rule=trigger_rule,
        args=convert_params_to_hydra_positional_arg(params=params),
    )


def submit_pyspark_job(
    cluster_name: str,
    task_id: str,
    python_main_module: str,
    args: list[str],
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
) -> DataprocSubmitJobOperator:
    """Submit a PySpark job to a Dataproc cluster.

    Args:
        cluster_name (str): Name of the cluster.
        task_id (str): Name of the task.
        python_main_module (str): Path to the Python module to run.
        args (list[str]): Arguments to pass to the Python module.
        trigger_rule (TriggerRule): Trigger rule for the task. Defaults to TriggerRule.ALL_SUCCESS.

    Returns:
        DataprocSubmitJobOperator: Airflow task to submit a PySpark job to a Dataproc cluster.
    """
    return submit_job(
        cluster_name=cluster_name,
        task_id=task_id,
        job_type="pyspark_job",
        trigger_rule=trigger_rule,
        job_specification={
            "main_python_file_uri": python_main_module,
            "args": args,
            "properties": {
                "spark.jars": "/opt/conda/miniconda3/lib/python3.10/site-packages/hail/backend/hail-all-spark.jar",
                "spark.driver.extraClassPath": "/opt/conda/miniconda3/lib/python3.10/site-packages/hail/backend/hail-all-spark.jar",
                "spark.executor.extraClassPath": "./hail-all-spark.jar",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.kryo.registrator": "is.hail.kryo.HailKryoRegistrator",
            },
        },
    )


def submit_job(
    cluster_name: str,
    task_id: str,
    job_type: str,
    job_specification: dict[str, Any],
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
) -> DataprocSubmitJobOperator:
    """Submit an arbitrary job to a Dataproc cluster.

    Args:
        cluster_name (str): Name of the cluster.
        task_id (str): Name of the task.
        job_type (str): Type of the job to submit.
        job_specification (dict[str, Any]): Specification of the job to submit.
        trigger_rule (TriggerRule): Trigger rule for the task. Defaults to TriggerRule.ALL_SUCCESS.

    Returns:
        DataprocSubmitJobOperator: Airflow task to submit an arbitrary job to a Dataproc cluster.
    """
    return DataprocSubmitJobOperator(
        task_id=task_id,
        region=GCP_REGION,
        project_id=GCP_PROJECT_GENETICS,
        job={
            "job_uuid": f"airflow-{task_id}",
            "reference": {"project_id": GCP_PROJECT_GENETICS},
            "placement": {"cluster_name": cluster_name},
            job_type: job_specification,
        },
        trigger_rule=trigger_rule,
    )


def delete_cluster(cluster_name: str) -> DataprocDeleteClusterOperator:
    """Generate an Airflow task to delete a Dataproc cluster.

    Args:
        cluster_name (str): Name of the cluster.

    Returns:
        DataprocDeleteClusterOperator: Airflow task to delete a Dataproc cluster.
    """
    return DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=GCP_PROJECT_GENETICS,
        cluster_name=cluster_name,
        region=GCP_REGION,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )


def generate_dataproc_task_chain(
    cluster_name: str,
    cluster_init_script: str,
    cluster_metadata: dict[str, str],
    tasks: list[DataprocSubmitJobOperator],
) -> Any:
    """For a list of Dataproc tasks, generate a complete chain of tasks.

    This function adds create_cluster, install_dependencies to the task that does not have any upstream tasks (first one in the DAG)
    and adds delete_cluster tasks to the task that does not have any downstream tasks (last one in the DAG)

    Args:
        cluster_name (str): Name of the cluster.
        cluster_init_script (str): URI to the cluster initialization script.
        cluster_metadata: (dict[str, str]): METADATA to fill into the cluster during initialization.
        tasks (list[DataprocSubmitJobOperator]): List of tasks to execute.

    Returns:
        list[DataprocSubmitJobOperator]: list of input tasks with muted chain.
    """
    create_cluster_task = create_cluster(
        cluster_name,
        cluster_metadata=cluster_metadata,
        cluster_init_script=cluster_init_script,
    )
    delete_cluster_task = delete_cluster(cluster_name)
    for task in tasks:
        if not task.get_direct_relatives(upstream=True):
            task.set_upstream(create_cluster_task)
        if not task.get_direct_relatives(upstream=False):
            task.set_downstream(delete_cluster_task)

    return tasks


def generate_dag(cluster_name: str, tasks: list[DataprocSubmitJobOperator]) -> Any:
    """For a list of tasks, generate a complete DAG.

    Args:
        cluster_name (str): Name of the cluster.
        tasks (list[DataprocSubmitJobOperator]): List of tasks to execute.

    Returns:
        Any: Airflow DAG.
    """
    create_cluster_task = create_cluster(cluster_name)
    delete_cluster_task = delete_cluster(cluster_name)
    for task in tasks:
        if not task.get_direct_relatives(upstream=True):
            task.set_upstream(create_cluster_task)
        if not task.get_direct_relatives(upstream=False):
            task.set_downstream(delete_cluster_task)

    return tasks


def reinstall_dependencies(
    cluster_name: str, cluster_init_script: str
) -> DataprocSubmitJobOperator:
    """Force install dependencies on a Dataproc cluster.

    Args:
        cluster_name (str): Name of the cluster.
        cluster_init_script (str): Name of the script to run in the cluster to update the dependencies.

    Returns:
        DataprocSubmitJobOperator: Airflow task to install dependencies on a Dataproc cluster.
    """
    cluster_init_script_name = GCSPath(cluster_init_script).segments["filename"]
    return submit_job(
        cluster_name=cluster_name,
        task_id="install_dependencies",
        job_type="pig_job",
        job_specification={
            "jar_file_uris": [cluster_init_script],
            "query_list": {
                "queries": [
                    f"sh chmod 750 $PWD/{cluster_init_script_name}",
                    f"sh $PWD/{cluster_init_script_name}",
                ]
            },
        },
    )

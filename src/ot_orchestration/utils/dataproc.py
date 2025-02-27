"""Airflow boilerplate code that interfaces with Dataproc operators which can be shared by several DAGs."""

from __future__ import annotations

import logging
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from ot_orchestration.utils import convert_params_to_hydra_positional_arg, random_id

# from ot_orchestration.utils import GCSPath
from ot_orchestration.utils.common import (
    GCP_AUTOSCALING_POLICY,
    GCP_DATAPROC_IMAGE,
    GCP_EFM_AUTOSCALING_POLICY,
    GCP_PROJECT_GENETICS,
    GCP_REGION,
    GCP_ZONE,
    GENTROPY_CLI_SCRIPT,
    GENTROPY_CLUSTER_INIT_SCRIPT,
)
from ot_orchestration.utils.labels import Labels
from ot_orchestration.utils.path import GCSPath

log: logging.Logger = logging.getLogger(__name__)


def create_cluster(
    cluster_name: str,
    project_id: str = GCP_PROJECT_GENETICS,
    master_machine_type: str = "n1-highmem-16",
    worker_machine_type: str = "n1-highmem-16",
    num_workers: int = 2,
    num_preemptible_workers: int = 0,
    num_local_ssds: int = 1,
    autoscaling_policy: str = GCP_AUTOSCALING_POLICY,
    master_disk_size: int = 500,
    cluster_init_script: str | None = GENTROPY_CLUSTER_INIT_SCRIPT,
    cluster_metadata: dict[str, str] | None = None,
    allow_efm: bool = False,
    idle_delete_ttl: int = 30 * 60,
    labels: Labels | None = None,
    task_id: str = "create_cluster",
    **kwargs: Any,
) -> DataprocCreateClusterOperator:
    """Generate an Airflow task to create a Dataproc cluster. Common parameters are reused, and varying parameters can be specified as needed.

    Args:
        cluster_name (str): Name of the cluster.
        project_id (str): Project ID. Defaults to GCP_PROJECT_GENETICS.
        master_machine_type (str): Machine type for the master node. Defaults to "n1-highmem-8".
        worker_machine_type (str): Machine type for the worker nodes. Defaults to "n1-standard-16".
        num_workers (int): Number of worker nodes. Defaults to 2.
        num_preemptible_workers (int): Number of preemptible worker nodes. Defaults to 0.
        num_local_ssds (int): How many local SSDs to attach to each worker node, both primary and secondary. Defaults to 1.
        autoscaling_policy (str): Name of the autoscaling policy to use. Defaults to GCP_AUTOSCALING_POLICY.
        master_disk_size (int): Size of the master node's boot disk in GB. Defaults to 500.
        cluster_init_script (str | None): Cluster initialization scripts.
        cluster_metadata (str | None): Cluster METADATA.
        allow_efm (bool): Wether to allow for Enhanced Flexibility Mode in spark cluster to store the shuffle partitions in the primary workers only.
        idle_delete_ttl (int): Time in seconds to wait before deleting the cluster after it becomes idle. Defaults to 30 minutes.
        labels (Labels): Optional labels to add to the cluster.
        task_id (str): task id used to during the dataproc cluster creation
        **kwargs (Any): Other parameters to the ClusterGenerator.

        NOTE: When `allow_efm` is enabled, the autoscaling policy can not use the graceful decommissioning for primary workers!
        NOTE: When `allow_efm` is enabled, the ratio between primary and secondary workers should not be small (1:10) at least.
        NOTE: When `allow_efm` is enabled, the size of primary workers disks should be increased and set to use the pd-ssd
        To see more about EFM see https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/enhanced-flexibility-mode

    Returns:
        DataprocCreateClusterOperator: Airflow task to create a Dataproc cluster.
    """
    labels = labels or Labels()

    # Create base cluster configuration.
    properties = {
        "spark:spark.sql.adaptive.enabled": "true",
        "spark:spark.shuffle.service.enabled": "true",
    }
    if allow_efm:
        properties = {
            "dataproc:efm.spark.shuffle": "primary-worker",
            "spark:spark.sql.adaptive.enabled": "true",
            "spark:spark.sql.files.maxPartitionBytes": "1073741824",  # value proposed by the Dataproc documentation. See EFM in docstring.
            "yarn:spark.shuffle.io.serverThreads": "50",  # ensure more threads can write default for n-standard-16 is 2 * (16 cores) threads
            "spark:spark.shuffle.io.numConnectionsPerPeer": "5",
            "spark:spark.stage.maxConsecutiveAttempts": "10",  # defaults to 4, this is in case the master was lost
            "spark:spark.task.maxFailures": "10",
        }

    cluster_config = ClusterGenerator(
        num_masters=3 if allow_efm else 1,  # allows to run the dataproc cluster in HA mode.
        project_id=project_id,
        zone=GCP_ZONE,
        master_machine_type=master_machine_type,
        worker_machine_type=worker_machine_type,
        worker_disk_type="pd-ssd",
        master_disk_size=master_disk_size,
        worker_disk_size=1024 * 2,
        num_preemptible_workers=num_preemptible_workers,
        num_workers=num_workers,
        image_version=GCP_DATAPROC_IMAGE,
        internal_ip_only=False,
        enable_component_gateway=True,
        metadata=cluster_metadata,
        idle_delete_ttl=idle_delete_ttl,
        init_actions_uris=[cluster_init_script] if cluster_init_script else None,
        autoscaling_policy=get_autoscaling_policy(
            policy_name=autoscaling_policy,
            project=project_id,
        ),
        properties=properties,
        **kwargs,
    ).make()

    # If specified, amend the configuration to include local SSDs for worker nodes.
    if num_local_ssds:
        for worker_section in ("worker_config", "secondary_worker_config"):
            # Create a disk config section if it does not exist.
            cluster_config[worker_section].setdefault("disk_config", {})
            # Specify the number of local SSDs.
            cluster_config[worker_section]["disk_config"]["num_local_ssds"] = num_local_ssds
    # Return the cluster creation operator.
    return DataprocCreateClusterOperator(
        task_id=task_id,
        project_id=project_id,
        cluster_config=cluster_config,
        region=GCP_REGION,
        cluster_name=cluster_name,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        labels=labels.as_dict(),
    )


def get_autoscaling_policy(
    *,
    policy_name: str = GCP_AUTOSCALING_POLICY,
    region: str = GCP_REGION,
    project: str = GCP_PROJECT_GENETICS,
    allow_efm: bool = False,
) -> str:
    """Get the autoscaling policy full path."""
    if allow_efm and policy_name == GCP_AUTOSCALING_POLICY:
        policy_name = GCP_EFM_AUTOSCALING_POLICY
    return f"projects/{project}/regions/{region}/autoscalingPolicies/{policy_name}"


def submit_gentropy_step(
    cluster_name: str,
    step_name: str,
    python_main_module: str = GENTROPY_CLI_SCRIPT,
    project_id: str = GCP_PROJECT_GENETICS,
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
    params: dict[str, Any] | None = None,
    labels: Labels | None = None,
) -> DataprocSubmitJobOperator:
    """Submit a PySpark job from a gentropy step to execute a specific CLI step.

    Args:
        cluster_name (str): Name of the cluster.
        step_name (str): Name of the gentropy step to run.
        python_main_module (str): GCS path to the gentropy CLI wrapper script.
        project_id (str): Project ID. Defaults to GCP_PROJECT_GENETICS.
        trigger_rule (TriggerRule): Trigger rule for the task. Defaults to TriggerRule.ALL_SUCCESS.
        params (list[str]): Optional parameters to append to the gentropy step, must be in key:value.
        labels (Labels): Optional labels to add to the job.

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
    labels = labels or Labels()
    log.info(f"Sending {step_name} to {cluster_name} with {params}")

    return submit_pyspark_job(
        cluster_name=cluster_name,
        task_id=step_name,
        project_id=project_id,
        python_main_module=python_main_module,
        trigger_rule=trigger_rule,
        args=convert_params_to_hydra_positional_arg(params=params, dataproc=True),
        labels=labels,
    )


def submit_pyspark_job(
    cluster_name: str,
    task_id: str,
    python_main_module: str,
    args: list[str],
    project_id: str = GCP_PROJECT_GENETICS,
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
    labels: Labels | None = None,
) -> DataprocSubmitJobOperator:
    """Submit a PySpark job to a Dataproc cluster.

    Args:
        cluster_name (str): Name of the cluster.
        task_id (str): Name of the task.
        python_main_module (str): Path to the Python module to run.
        args (list[str]): Arguments to pass to the Python module.
        project_id (str): Project ID. Defaults to GCP_PROJECT_GENETICS.
        trigger_rule (TriggerRule): Trigger rule for the task. Defaults to TriggerRule.ALL_SUCCESS.
        labels (Labels): Optional labels to add to the job.

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
                "spark.jars": "/opt/conda/miniconda3/lib/python3.11/site-packages/hail/backend/hail-all-spark.jar",
                "spark.driver.extraClassPath": "/opt/conda/miniconda3/lib/python3.11/site-packages/hail/backend/hail-all-spark.jar",
                "spark.executor.extraClassPath": "./hail-all-spark.jar",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.kryo.registrator": "is.hail.kryo.HailKryoRegistrator",
            },
        },
        project_id=project_id,
        labels=labels,
    )


def submit_job(
    cluster_name: str,
    task_id: str,
    job_type: str,
    job_specification: dict[str, Any],
    project_id: str = GCP_PROJECT_GENETICS,
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
    labels: Labels | None = None,
) -> DataprocSubmitJobOperator:
    """Submit an arbitrary job to a Dataproc cluster.

    Args:
        cluster_name (str): Name of the cluster.
        task_id (str): Name of the task.
        job_type (str): Type of the job to submit.
        job_specification (dict[str, Any]): Specification of the job to submit.
        project_id (str): Project ID. Defaults to GCP_PROJECT_GENETICS.
        trigger_rule (TriggerRule): Trigger rule for the task. Defaults to TriggerRule.ALL_SUCCESS.
        labels (Labels): Optional labels to add to the job.

    Returns:
        DataprocSubmitJobOperator: Airflow task to submit an arbitrary job to a Dataproc cluster.
    """
    labels = labels or Labels()
    job_id = f"{cluster_name}-{task_id}-{random_id()}"

    return DataprocSubmitJobOperator(
        task_id=task_id,
        region=GCP_REGION,
        project_id=project_id,
        job={
            "job_uuid": f"airflow-{task_id}",
            "reference": {"project_id": project_id, "job_id": job_id},
            "placement": {"cluster_name": cluster_name},
            job_type: job_specification,
            "labels": labels.as_dict(),
        },
        trigger_rule=trigger_rule,
    )


def delete_cluster(
    cluster_name: str,
    task_id: str = "delete_cluster",
    project_id: str = GCP_PROJECT_GENETICS,
) -> DataprocDeleteClusterOperator:
    """Generate an Airflow task to delete a Dataproc cluster.

    Args:
        cluster_name (str): Name of the cluster.
        task_id (str): Dataproc delete cluster task id.
        project_id (str): Project ID. Defaults to GCP_PROJECT_GENETICS.

    Returns:
        DataprocDeleteClusterOperator: Airflow task to delete a Dataproc cluster.
    """
    return DataprocDeleteClusterOperator(
        task_id=task_id,
        project_id=project_id,
        cluster_name=cluster_name,
        region=GCP_REGION,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )


def generate_dataproc_task_chain(
    tasks: list[BaseOperator],
    *,
    create: bool = True,
    delete: bool = True,
    **kwargs: Any,
) -> list[BaseOperator]:
    """For a list of Dataproc tasks, generate a complete chain of tasks.

    This function adds create_cluster, install_dependencies to the task that does not have any upstream tasks (first one in the DAG)
    and adds delete_cluster tasks to the task that does not have any downstream tasks (last one in the DAG)

    Args:
        tasks (list[BaseOperator]): List of tasks to execute.
        create (bool): Indicate if the cluster should be created prior the first dataproc task. Defaults to True.
        delete (bool): Indicate if the cluster should be deleted after the last dataproc task. Defaults to True.
        **kwargs (Any): keyword arguments passed to the `create_cluster`. Should always contain the cluster_name.

    Returns:
        list[BaseOperator]: list of input tasks with muted chain.
    """
    if create:
        create_cluster_task = create_cluster(**kwargs)
        for task in tasks:
            if not task.get_direct_relatives(upstream=True):
                task.set_upstream(create_cluster_task)
    if delete:
        delete_cluster_task = delete_cluster(kwargs["cluster_name"])
        for task in tasks:
            if not task.get_direct_relatives(upstream=False):
                task.set_downstream(delete_cluster_task)

    return tasks


def reinstall_dependencies(
    cluster_name: str,
    cluster_init_script: str,
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

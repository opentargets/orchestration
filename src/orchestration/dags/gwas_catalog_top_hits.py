"""Airflow DAG for the preprocessing of GWAS Catalog's top hits."""

from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.decorators import task_group
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.models.taskmixin import DAGNode
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

from orchestration.dags.config.staging_config import GentropyPipelineConfig, StagingPipelineStepParams
from orchestration.operators.dataproc import (
    ClusterConfig,
    CreateClusterOperator,
    DeleteClusterOperator,
    GentropyJobBuilder,
    SubmitJobOperator,
)
from orchestration.operators.gce import ComputeEngineRunContainerizedWorkloadSensor, DeleteInstanceOperator
from orchestration.operators.gcs import UploadStringOperator
from orchestration.types import ConfigNode, Environment, EnvironmentSpec
from orchestration.utils import chain_dependencies, find_environment_vars, read_yaml_config, to_yaml
from orchestration.utils.common import GCP_PROJECT_GENETICS, shared_dag_args, shared_dag_kwargs
from orchestration.utils.labels import Labels


def chain_steps(steps: dict[str, StagingPipelineStepParams], task_groups: dict[str, DAGNode]) -> None:
    for step_name, params in steps.items():
        step_deps = params.depends_on
        step_node = task_groups.get(step_name)
        if step_node is None:
            raise ValueError(f"Could not find the step {step_name} in the DAG.")
        if step_deps:
            dep_nodes = [task_groups.get(dep_name) for dep_name in step_deps]
            for dep_node in dep_nodes:
                if dep_node is None:
                    raise ValueError(f"Could not set all dependencies for {step_node}")
                step_node.set_upstream(dep_node)


def resource_name(prefix: str, step_name: str) -> str:
    tools = ["gentroutils", "gentropy"]
    for t in tools:
        if t in step_name:
            step_name.removeprefix(t)
            return f"{prefix}-{step_name}"
    raise ValueError(f"Not found tool prefix in {step_name}")


SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "config" / "gwas_catalog_top_hits.yaml"
with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog top hits",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
) as dag:
    task_groups = {}
    cluster_task_groups = {}
    config = GentropyPipelineConfig.read_config(str(SOURCE_CONFIG_FILE_PATH))
    resource_prefix = "top_hits"
    for step_name, step_config in config.steps.items():
        resource = resource_name(prefix=resource_prefix, step_name=step_name)

        if step_name.startswith("gentropy"):
            if not step_config.cluster:
                raise ValueError(f"Failed to find cluster for gentropy step {step_name}")
            create_cluster_task_id = f"create_cluster_{step_config.cluster}"
            c = EmptyOperator(task_id=create_cluster_task_id)

            delete_cluster_task_id = f"delete_cluster_{step_config.cluster}"
            d = EmptyOperator(task_id=delete_cluster_task_id)

            if create_cluster_task_id in cluster_task_groups:
                cluster_task_groups[create_cluster_task_id].append

            @task_group(group_id=step_name)
            def gentropy_step(step_config: StagingPipelineStepParams, step_name: str):
                EmptyOperator(task_id=f"submit_{step_name}")

            s = gentropy_step(step_config, step_name)
            task_groups[step_name] = s

            chain(c, Label("Run task"), s, Label("Delete cluster"), d)

        if step_name.startswith("gentroutils"):

            @task_group(group_id=step_name)
            def gentroutils_step(step_config: StagingPipelineStepParams, step_name: str):
                u = EmptyOperator(task_id=f"upload_config_{step_name}")
                r = EmptyOperator(task_id=f"run_{step_name}")
                t = EmptyOperator(task_id=f"terminate_{step_name}")

                chain(u, Label("Run task"), r, Label("Terminate machine"), t)

            task_groups[step_name] = gentroutils_step(step_config, step_name)

    chain_steps(config.steps, task_groups)

    # chainer = Chainer()

    # config = read_yaml_config(SOURCE_CONFIG_FILE_PATH)
    # env_spec: list[EnvironmentSpec] = config["environment_specs"]
    # env: Environment = config["env"]
    # sentinels = find_environment_vars(env_spec, env)
    # config = read_yaml_config(SOURCE_CONFIG_FILE_PATH, sentinels)
    # steps: list[ConfigNode] = config.get("steps", [])

    # cluster_name = "gwas-catalog-top-hits"
    # cc = ClusterConfig(**config.get("dataproc", {}))
    # labels = Labels(extra={"dag": dag.dag_id}, project=GCP_PROJECT_GENETICS)
    # c = CreateClusterOperator(
    #     task_id=f"create_cluster {cluster_name}",
    #     project_id=cc.project_id,
    #     cluster_name=cluster_name,
    #     cluster_config=cc,
    #     labels=labels,
    # )
    # for step in steps:
    #     if step["tool"] == "gentropy":
    #         job = GentropyJobBuilder(
    #             main_python_file_uri="gs://genetics_etl_python_playground/initialisation/cli.py",
    #             params=step["params"],
    #             properties={
    #                 "spark.jars": "/opt/conda/miniconda3/lib/python3.11/site-packages/hail/backend/hail-all-spark.jar",
    #                 "spark.driver.extraClassPath": "/opt/conda/miniconda3/lib/python3.11/site-packages/hail/backend/hail-all-spark.jar",
    #                 "spark.executor.extraClassPath": "./hail-all-spark.jar",
    #                 "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    #                 "spark.kryo.registrator": "is.hail.kryo.HailKryoRegistrator",
    #             },
    #         ).build()
    #         j = SubmitJobOperator(
    #             task_id=step["id"],
    #             project_id=cc.project_id,
    #             cluster_name=cluster_name,
    #             step_name=step["id"],
    #             py_spark_job=job,
    #             labels=labels,
    #         )
    #     if step["tool"] == "gentroutils":

    #         @task_group(group_id=step["id"])
    #         def gentroutils_step(step: ConfigNode) -> None:
    #             release_date = datetime.now().strftime("%Y%m%d%H%M%S")
    #             config_uri: str = config.get("staging_bucket") + f"/config/{release_date}/config.yaml"
    #             uc = UploadStringOperator(
    #                 task_id=f"upload_config_{step['id']}",
    #                 contents=to_yaml(step["params"]),
    #                 dst_uri=config_uri,
    #                 overwrite=True,
    #             )
    #             r = ComputeEngineRunContainerizedWorkloadSensor(
    #                 task_id=f"run {step['id']}",
    #                 instance_name=vm_name,
    #                 labels=labels,
    #                 container_image=gentroutils_container_image,
    #                 container_env=env_vars,
    #                 container_scopes=service_account_extra_scopes,
    #                 conteiner_files={config_uri: "/config.yaml"},
    #                 worker_dist_size_gb=worker_disk_size_gb,
    #                 deferrable=True,
    #             )
    #             t = DeleteInstanceOperator(
    #                 task_id=f"delete_vm_{step['id']}",
    #                 resource_id=vm_name,
    #                 project_id=cc.project_id,
    #             )
    #             chain(uc, r, t)

    #         chain

    # d = DeleteClusterOperator(
    #     task_id=f"delete_cluster {cluster_name}",
    #     project_id=cc.project_id,
    #     cluster_name=cluster_name,
    #     trigger_rule=TriggerRule.ALL_DONE,
    # )


if __name__ == "__main__":
    dag.test()

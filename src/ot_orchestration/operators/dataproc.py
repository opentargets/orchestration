"""Utility functions for working with Dataproc clusters."""

from __future__ import annotations

import inspect
from collections.abc import Sequence
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.context import Context
from google.cloud.dataproc_v1 import Cluster, JobReference
from google.cloud.dataproc_v1.types.jobs import Job, JobPlacement, SparkJob

from ot_orchestration.common import GCP_PROJECT_PLATFORM, GCP_REGION
from ot_orchestration.types import DataprocSpecs
from ot_orchestration.utils import random_id
from ot_orchestration.utils.dataproc import ClusterGenerator
from ot_orchestration.utils.labels import Labels


class DataprocCreateClusterConfigGenerateOperator(BaseOperator):
    """This operator creates the Dataproc cluster configuration.

    To see all available options check the `airflow.providers.google.cloud.operators.dataproc.ClusterGenerator`
    class initialization arguments at `airflow documentation <https://github.com/apache/airflow/blob/8b21b4e2126a1a3a514407b29003b7349ab8cf22/providers/google/src/airflow/providers/google/cloud/operators/dataproc.py#L120>`__.

    The operator allows for passing the DataprocSpecs object to the ClusterGenerator class.

    Args:
        specs (DataprocSpecs): Object containing dataproc specification.

    This operator:
    1. Inspects the required keys needed to generate the cluster with ClusterGenerator class.
    2. Drops all additional parameters provided by the user and warns the user about the additional parameters.

    The validation, although not necessary, allows the user to make sure that all configuration options passed
    downstream to the ClusterGenerator have valid keys. This can prevent from the typos creating cluster that does not meet
    the requested criteria.
    """

    def __init__(self, specs: DataprocSpecs, **kwargs) -> None:
        self.cluster_kwargs = specs
        self.log.debug("Requested following cluster configuration %s", self.cluster_kwargs)
        super().__init__(**kwargs)

    def execute(self, context: Context) -> Cluster:
        """Execute the operator."""
        cluster_generator_keys = set(inspect.signature(ClusterGenerator).parameters.keys())
        cg_kwargs: dict[str, Any] = {k: v for k, v in self.cluster_kwargs.items() if k in cluster_generator_keys}
        other_keys = {k for k in self.cluster_kwargs if k not in cluster_generator_keys}
        if other_keys:
            self.log.warning(
                "%s got unexpected %s that will be dropped before passing to ClusterGenerator.",
                type(self).__name__,
                other_keys,
            )
        cluster_config: dict = ClusterGenerator(**cg_kwargs).make()
        return Cluster(**cluster_config)


class PlatformETLCreateClusterOperator(DataprocCreateClusterOperator):
    """Create a new Dataproc cluster tailored for running Platform ETL.

    This class sets many default values and streamlines the cluster creation to
    the needs of ETL. Refer to the parent class for details on extending this one.

    For more information on how to use this operator, take a look at
    `the guide <https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataproc.html#howto-operator-dataproccreateclusteroperator>`_.

    Args:
        project_id: The ID of the Google cloud project in which
            to create the cluster. (templated)
        region: The specified region where the dataproc cluster is created.
        cluster_name: Name of the cluster to create
        cluster_config: Required. The cluster config to create.
            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.dataproc_v1.types.ClusterConfig`
        idle_delete_ttl: Optional. The duration to keep the cluster alive while idling.
        labels: Labels that will be assigned to created cluster. Please, notice that
            adding labels to ClusterConfig object in cluster_config parameter will not lead
            to adding labels to the cluster. Labels for the clusters could be only set by passing
            values to parameter of DataprocCreateCluster operator.
        labels: Labels that will be assigned to created cluster.
        metadata: Additional metadata that is provided to the method.
        gcp_conn_id: The connection ID to use connecting to Google Cloud.
        impersonation_chain: Optional service account to impersonate using short-term
            credentials, or chained list of accounts required to get the access_token
            of the last account in the list, which will be impersonated in the request.
            If set as a string, the account must grant the originating account
            the Service Account Token Creator IAM role.
            If set as a sequence, the identities from the list must grant
            Service Account Token Creator IAM role to the directly preceding identity, with first
            account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "region",
        "cluster_config",
        "virtual_cluster_config",
        "cluster_name",
        "labels",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str = GCP_PROJECT_PLATFORM,
        region: str = GCP_REGION,
        cluster_name: str,
        cluster_config: dict | Cluster | None = None,
        idle_delete_ttl: int = 7200,
        labels: dict[str, str] | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name
        self.idle_delete_ttl = idle_delete_ttl
        self.labels = labels or {}
        self.metadata = metadata
        self.cluster_config = cluster_config or self._create_cluster_config()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

        super().__init__(
            project_id=self.project_id,
            region=self.region,
            cluster_name=self.cluster_name,
            cluster_config=self.cluster_config,
            labels=self.labels,
            metadata=self.metadata,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            **kwargs,
        )

    def _create_cluster_config(self) -> dict:
        # This is the legacy cluster configuration for ETL dataproc clusters.
        # There are minor changes, but it must be fine-tuned as we start testing
        # the new pipeline runs.
        # The only changes so far are:
        # - Disks are now pd-ssd instead of pd-standard
        # - Idle delete TTL defaults to 7200 seconds so the cluster is deleted
        #   after 2 hours of inactivity. The reason for this is ETL clusters are
        #   not deleted if any of the tasks fail.
        return ClusterGenerator(
            project_id=GCP_PROJECT_PLATFORM,
            master_machine_type="n1-highmem-16",
            enable_component_gateway=True,
            master_disk_size=512,
            master_disk_type="pd-ssd",
            worker_machine_type="n1-highmem-64",
            num_workers=4,
            worker_disk_size=2000,
            worker_disk_type="pd-ssd",
            image_version="2.0-debian10",
            properties={"spark:spark.driver.memory": "478g"},
            idle_delete_ttl=self.idle_delete_ttl,
        ).make()

    def execute(self, context: Context) -> dict:
        """Execute the operator with added runtime label."""
        self.labels = Labels.from_dict(self.labels).add_dag_run_label(context).as_dict()
        return super().execute(context)


class PlatformETLSubmitJobOperator(DataprocSubmitJobOperator):
    """Submit a job to a cluster.

    Args:
        project_id: Optional. The ID of the Google Cloud project that the job belongs to.
        region: Optional. The Cloud Dataproc region in which to handle the request.
        cluster_name: Required. The cluster to submit the job to.
        step_name: Required. The name of the job.
        jar_uri: Required. The URL of the jar file for the job. Note it is named URL
            and not URI.
        config_uri: Required. The URL of the configuration file for the job. Note it is
            named URL and not URI.
        labels: Optional. The labels to associate with this job.
        gcp_conn_id:
        impersonation_chain: Optional service account to impersonate using short-term
            credentials, or chained list of accounts required to get the access_token
            of the last account in the list, which will be impersonated in the request.
            If set as a string, the account must grant the originating account
            the Service Account Token Creator IAM role.
            If set as a sequence, the identities from the list must grant
            Service Account Token Creator IAM role to the directly preceding identity, with first
            account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "region",
        "cluster_name",
        "step_name",
        "jar_uri",
        "config_uri",
        "labels",
        "impersonation_chain",
        "request_id",
    )

    def __init__(
        self,
        *,
        project_id: str = GCP_PROJECT_PLATFORM,
        region: str = GCP_REGION,
        cluster_name: str,
        step_name: str,
        jar_uri: str,
        config_uri: str,
        labels: dict[str, str] | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name
        self.step_name = step_name
        self.jar_uri = jar_uri
        self.config_uri = config_uri
        self.labels = labels or {}
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

        super().__init__(
            job={},
            region=self.region,
            project_id=self.project_id,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            **kwargs,
        )

    def execute(self, context):
        """Execute the operator."""
        config_filename = self.config_uri.split("/")[-1]
        job_id = f"{self.cluster_name}-{self.step_name}-{random_id()}"

        self.job = Job(
            reference=JobReference(project_id=self.project_id, job_id=job_id),
            placement=JobPlacement(cluster_name=self.cluster_name),
            spark_job=SparkJob(
                main_jar_file_uri=self.jar_uri,
                file_uris=[self.config_uri],
                args=[self.step_name],
                properties={
                    "spark.executor.extraJavaOptions": f"-Dconfig.file={config_filename} -XX:MaxPermSize=512m -XX:+UseCompressedOops",
                    "spark.driver.extraJavaOptions": f"-Dconfig.file={config_filename} -XX:MaxPermSize=512m -XX:+UseCompressedOops",
                },
            ),
            labels=Labels.from_dict(self.labels).add_dag_run_label(context).as_dict(),
        )

        return super().execute(context)

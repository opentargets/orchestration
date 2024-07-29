"""Custom Google Cloud Run operators."""

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunCreateJobOperator,
    CloudRunExecuteJobOperator,
)
from airflow.utils.decorators import apply_defaults
from google.cloud import logging, run_v2


class CloudRunExecuteJobWithLogsOperator(CloudRunExecuteJobOperator):
    """Custom operator to execute a Cloud Run job and fetch logs from it."""

    template_fields = ["project_id", "region", "job_name"]

    @apply_defaults
    def __init__(
        self,
        *args,
        project_id: str,
        region: str,
        job_name: str,
        **kwargs,
    ) -> None:
        super().__init__(
            project_id=project_id,
            region=region,
            job_name=job_name,
            *args,
            **kwargs,
        )
        self.project_id = project_id
        self.region = region
        self.job_name = job_name

    def execute(self, context) -> None:
        """Execute the Cloud Run job and then fetch logs."""
        try:
            super().execute(context)
        except AirflowException as e:
            raise e
        finally:
            client = logging.Client(project=self.project_id)
            query = f'resource.type = "cloud_run_job" resource.labels.job_name = "{self.job_name}"'
            entries = client.list_entries(
                filter_=query,
                order_by=logging.ASCENDING,
                page_size=100,
            )
            for entry in entries:
                self.log.info(entry.payload)


class CloudRunCreateJobExtendedOperator(CloudRunCreateJobOperator):
    """Custom operator to create a Cloud Run.

    This operator will create a Cloud Run with a given image, environment,
    limits, service account and configuration volume.
    """

    template_fields = [
        "project_id",
        "region",
        "job_name",
        "image",
        "env",
        "service_account",
        "config_bucket",
    ]

    def __init__(
        self,
        *args,
        task_id: str,
        project_id: str,
        region: str,
        job_name: str,
        image: str,
        env_map: dict[str, str] = {},
        limits_map: dict[str, str] = {},
        service_account: str | None = None,
        config_bucket: str | None = None,
        **kwargs,
    ) -> None:
        self.task_id = task_id
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.image = image
        self.env_map = env_map
        self.limits_map = limits_map
        self.service_account = service_account
        self.config_bucket = config_bucket

        self.limits = run_v2.ResourceRequirements(limits=limits_map)

        if self.config_bucket:
            gcs = run_v2.GCSVolumeSource(bucket=self.config_bucket)
            self.volumes = [run_v2.Volume(name="cfg", gcs=gcs)]
            self.volume_mounts = [run_v2.VolumeMount(name="cfg", mount_path="/config")]

        if self.env_map:
            self.env = [run_v2.EnvVar(name=k, value=v) for k, v in self.env_map.items()]

        self.job = run_v2.Job(
            launch_stage="BETA",
            template=run_v2.ExecutionTemplate(
                template=run_v2.TaskTemplate(
                    max_retries=0,
                    containers=[
                        run_v2.Container(
                            image=self.image,
                            resources=self.limits,
                        )
                    ],
                    timeout=f"{str(5*60*60)}s",
                )
            ),
        )

        if self.env:
            self.job.template.template.containers[0].env = self.env

        if self.volume_mounts:
            self.job.template.template.volumes = self.volumes
            self.job.template.template.containers[0].volume_mounts = self.volume_mounts

        if self.service_account:
            self.job.template.template.service_account = self.service_account

        super().__init__(
            task_id=self.task_id,
            project_id=self.project_id,
            region=self.region,
            job_name=self.job_name,
            job=self.job,
            *args,
            **kwargs,
        )

    def execute(self, context) -> None:
        """Create the Cloud Run job with the configuration."""
        super().execute(context)

        self.log.info(f"Cloud Run job created with configuration: {self.job}")

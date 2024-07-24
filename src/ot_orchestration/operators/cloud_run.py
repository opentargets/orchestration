"""Custom operator to execute a Cloud Run job and fetch logs from it."""

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)
from airflow.utils.decorators import apply_defaults
from google.cloud import logging


class CloudRunExecuteJobWithLogsOperator(CloudRunExecuteJobOperator):
    """Custom operator to execute a Cloud Run job and fetch logs from it."""

    template_fields = ["project_id", "region", "job_name"]

    @apply_defaults
    def __init__(self, *args, project_id, region, job_name, **kwargs) -> None:
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
            entries = client.list_entries(filter_=query, order_by=logging.ASCENDING)
            for entry in entries:
                self.log.info(entry.payload)

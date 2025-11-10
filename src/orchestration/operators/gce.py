"""Custom sensor that runs a containerized workload on a Google Compute Engine instance."""

from __future__ import annotations

import asyncio
import datetime
import logging
import random
import time
from collections.abc import Sequence
from functools import cached_property
from textwrap import dedent
from typing import TYPE_CHECKING

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.compute import ComputeEngineHook
from airflow.providers.google.cloud.operators.compute import ComputeEngineDeleteInstanceOperator
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context
from google.api_core.exceptions import ResourceExhausted, RetryError
from google.api_core.extended_operation import ExtendedOperation
from google.cloud import compute_v1, logging_v2
from google.cloud import logging as google_logging
from google.cloud.compute_v1 import types
from google.cloud.logging_v2.services.logging_service_v2 import LoggingServiceV2AsyncClient

from orchestration.utils.common import GCP_PROJECT_PLATFORM, GCP_ZONE
from orchestration.utils.labels import Labels

if TYPE_CHECKING:
    from typing import Any

CONTAINER_NAME = "workload_container"
LOGGING_REQUEST_INTERVAL = 2
LOGGING_REQUEST_MAX_INTERVAL = 180

# WARNING
# After any change in deferrable operators, you must restart the airflow triggerer
# container to apply the changes with:
# docker compose restart airflow-trigger
# hopefully this will save you some time debugging due to stupid airflow quirks


def wait_for_extended_operation(
    operation: ExtendedOperation,
    verbose_name: str = "operation",
    timeout: int | None = 300,
    log: logging.Logger | None = None,
) -> Any:
    """Waits for the extended (long-running) operation to complete.

    If the operation is successful, it will return its result.
    If the operation ends with an error, an exception will be raised.
    If there were any warnings during the execution of the operation
    they will be logged.

    Args:
        operation: a long-running operation you want to wait on.
        verbose_name: (optional) a more verbose name of the operation,
            used only during error and warning reporting.
        timeout: how long (timedelta) to wait for operation to finish.
            If None, wait indefinitely.
        log: (optional) a logger to use for logging.

    Returns:
        Whatever the operation.result() returns.

    Raises:
        This method will raise the exception received from `operation.exception()`
        or RuntimeError if there is no exception set, but there is an `error_code`
        set for the `operation`.

        In case of an operation taking longer than `timeout` seconds to complete,
        a `concurrent.futures.TimeoutError` will be raised.
    """
    if log is None:
        log = logging.getLogger(__name__)

    result = operation.result(timeout=timeout)

    if operation.error_code:
        log.error(
            f"Error during {verbose_name}: [Code: {operation.error_code}]: {operation.error_message}",
        )
        raise operation.exception() or RuntimeError(operation.error_message)

    if operation.warnings:
        log.warning(f"Warnings during {verbose_name}")
        for warning in operation.warnings:
            log.warning(f"{warning.code}: {warning.message}")

    return result


def _backoff(request_interval: float) -> float:
    return min(request_interval * random.uniform(2, 2.5), LOGGING_REQUEST_MAX_INTERVAL)


class RateLimitedLoggingClient(logging_v2.Client):
    """Client for the Google Cloud Logging service with rate limiting.

    This client will wait for a minimum interval between requests to avoid
    hitting the rate limits of the Google Cloud Logging service.

    We are hitting logging API rate limits when we are trying to list log entries
    to copy them to the airflow logs, as there are numerous concurrent requests
    when running all of PIS steps in parallel.

    This may delay the logs from being copied to the Airflow logs for steps with
    a large number of log entries, but it will prevent the rate limit errors.
    """

    def __init__(self, log: logging.Logger, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log = log
        self.request_interval = LOGGING_REQUEST_INTERVAL

    def list_entries(self, *args, **kwargs):
        """List log entries and retries request that get rate-limited."""
        entries = None

        while True:
            try:
                entries = super().list_entries(*args, **kwargs)
                self.request_interval = _backoff(self.request_interval)
                break
            except ResourceExhausted:
                self.log.warning(
                    "Rate limit for logging api exceeded, waiting for %d seconds",
                    self.request_interval,
                )
                time.sleep(self.request_interval)
                self.request_interval *= 2

        self.request_interval = LOGGING_REQUEST_INTERVAL
        return entries


class CloudLoggingHook(GoogleBaseHook):
    """Hook for the Google Logging service.

    Args:
        gcp_conn_id: The connection ID to use when connecting to Google Cloud.
        impersonation_chain: Optional service account or chain to impersonate.
    """

    def __init__(
        self,
        api_version: str = "v2",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        self._client: RateLimitedLoggingClient | None = None
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version

    def get_conn(self) -> RateLimitedLoggingClient:
        """Return the Google Cloud Logging service client."""
        if self._client is None:
            self._client = RateLimitedLoggingClient(
                log=self.log,
                project=self.project_id,
                credentials=self.get_credentials(),
                client_info=CLIENT_INFO,
            )
        return self._client


class CloudLoggingAsyncHook(GoogleBaseHook):
    """Async hook for the Google Logging service.

    Args:
        gcp_conn_id: The connection ID to use when connecting to Google Cloud.
        impersonation_chain: Optional service account or chain to impersonate.
    """

    def __init__(
        self,
        api_version: str = "v2",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        self._client: LoggingServiceV2AsyncClient | None = None
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version
        self.request_interval = LOGGING_REQUEST_INTERVAL

    def get_conn(self) -> LoggingServiceV2AsyncClient:
        """Return the Google Cloud Logging service client."""
        if self._client is None:
            self._client = LoggingServiceV2AsyncClient(
                credentials=self.get_credentials(),
                client_info=CLIENT_INFO,
            )
        return self._client

    async def get_exit_code(
        self,
        project_name: str,
        instance_name: str,
        start_time: datetime.datetime,
    ) -> int | None:
        """Get the exit code of the startup script of a Google Compute Engine instance.

        According to Google Cloud documentation in `viewing the output of a Linux startup script
        <https://cloud.google.com/compute/docs/instances/startup-scripts/linux#viewing-output>`__,

        The exit code of the startup script should be visible in serial port 1 or the system logs,
        but it is not there. The only way to get the exit code is to search the logs for the message
        by using the logging API.

        The exit code is the number that follows the string "exit status" in the message. To harden
        the query, we are using the regex:

        startup-script[\\w\":\\s]*exit status [0-9]+

        This is because the message changes depending on the script exiting successfully:

        startup-script exit status 0

        And when the script fails:

        Script "startup-script" failed with error: exit status 1
        """  # noqa: D301
        client = self.get_conn()
        timestamp = start_time.isoformat()
        query = f'resource.type="gce_instance" labels.instance_name="{instance_name}" timestamp>"{timestamp}" jsonPayload.message=~"startup-script[\w\\\":\s]*exit status [0-9]+"'  # type: ignore # fmt: skip  # noqa: Q004, W605
        log_pages = None

        while True:
            try:
                log_pages = await client.list_log_entries(
                    resource_names=[f"projects/{project_name}"],
                    filter=query,
                    timeout=300,
                )
                self.request_interval = LOGGING_REQUEST_INTERVAL  # Reset the interval on successful requests
                break
            except ResourceExhausted:
                self.log.warning(
                    "Rate limit for logging api exceeded, waiting for %d seconds",
                    self.request_interval,
                )
                await asyncio.sleep(self.request_interval)
                self.request_interval = _backoff(self.request_interval)
            except RetryError as e:
                self.log.warning(
                    "Error occurred while fetching log entries: %s, retrying after %d seconds",
                    e,
                    self.request_interval,
                )
                await asyncio.sleep(self.request_interval)
                self.request_interval = _backoff(self.request_interval)

        logs = None
        try:
            logs = await anext(log_pages.pages, None)
        except Exception as e:
            self.log.error("Error occurred while fetching log entries: %s", e)

        if logs and logs.entries:
            entry = logs.entries[0]
            return int(entry.json_payload["message"].split("exit status", 1)[1].strip())  # type: ignore[index]

        self.log.info("No log entries with an exit status found yet.")
        return None


class ComputeEngineRunContainerizedWorkloadSensor(BaseSensorOperator):
    """Runs a containerized workload on a Google Compute Engine instance, and waits for it to finish.

    The sensor also takes care of the creation of the instance, using COOS as the base image, and
    uses a startup script to run the container passed in `container_image`. Arguments and environment
    can be passed to the container using `container_args` and `container_env` respectively. The
    sensor will wait for the startup script to finish and return the exit code.

    Be aware this sensor _MUST_ run in deferrable mode (explicitly setting `deferrable=True`). The
    poke method is not implemented, and the sensor will never return True when run in blocking mode.

    To enable non-deferrable mode we must implement the poke method properly.

    Args:
        project_id: Optional, the Google Cloud project ID where the job is.
            If set to None or missing, the default project_id for platform is used (GCP_PROJECT_PLATFORM).
        zone: The zone where the instance will be created (default is GCP_ZONE).
        instance_name: Name of the instance name that will run the workload.
        labels: Labels to apply to the instance. See the `Labels` class for more information.
        container_image: Container image to run.
        container_command: Command to run inside the container (optional).
        container_args: Arguments to pass to the container (optional).
        container_env: Environment variables to pass to the container (optional).
        container_scopes: A list of extra scopes to add to the service account if any are needed.
        container_files: Files to copy to the instance (optional). This is a dictionary where
            the key is a GCS path in the form `gs://bucket/path/to/file` and the value is the path
            where the file will be copied to in the instance. This is intended for small files needed
            to run the workload (like the configuration). Large files should be downloaded by the
            workload itself. The paths specified as values will be relative to the `/home/app` directory
            and all the parents will be created if they don't exist. Inside the docker container,
            they will be mounted under the root directory.
        machine_type: Machine type to use for the instance (default e2-standard-2).
        work_disk_size_gb: If present, a second disk with the specified size in GB will be
            attached to the instance besides the boot disk, to be used by the workload. The disk will
            be formatted with ext4 and mounted under `/mnt/disks/work`. The instance will have write
            permissions to the disk. The disk will be deleted when the instance is deleted.
        gcp_conn_id: The connection ID to use when connecting to Google Cloud.
        impersonation_chain: Optional service account or chain to impersonate.
        deferrable: If True, run the sensor in deferrable mode.
        poll_interval: Time (seconds) to wait between checks for the job status.
    """

    template_fields: Sequence[str] = (
        "instance_name",
        "labels",
        "container_image",
        "container_command",
        "container_args",
        "container_env",
        "container_files",
    )

    def __init__(
        self,
        *,
        project: str = GCP_PROJECT_PLATFORM,
        zone: str = GCP_ZONE,
        instance_name: str,
        labels: Labels | None = None,
        container_image: str,
        container_command: str = "",
        container_args: list[str] | None = None,
        container_env: dict[str, str] | None = None,
        container_scopes: list[str] | None = None,
        container_files: dict[str, str] | None = None,
        machine_type: str = "n1-standard-16",
        work_disk_size_gb: int = 0,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project = project
        self.zone = zone
        self.instance_name = instance_name
        self.labels = labels or Labels()
        self.container_image = container_image
        self.container_command = container_command
        self.container_args = container_args
        self.container_env = container_env
        self.container_scopes = container_scopes or []
        self.container_files = container_files or {}
        self.machine_type = machine_type
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.work_disk_size_gb = work_disk_size_gb
        self.deferrable = deferrable
        self.poll_interval = poll_interval

    def build_env_params(self):
        """Build the environment parameters for the docker run command."""
        if not self.container_env:
            return "\\"
        return ("\n").join([f"    -e {k}={v} \\" for k, v in self.container_env.items()])

    def build_volume_params(self):
        """Build the volume parameters for the docker run command."""
        if self.container_files == {}:
            return "\\"
        vs = [f"    -v /home/app/{p.lstrip('/')}:{p} \\" for p in self.container_files.values()]
        if self.work_disk_size_gb:
            vs.append("    -v /mnt/disks/work:/mnt/disks/work \\")
        return ("\n").join(vs)

    def startup_script(self):
        """Build the startup script for the instance.

        The startup script will:

        1. Create a user `app`, its home, and add it to the `docker` group.
        2. Copy the files specified in `container_files` from GCS to the instance.
        3. Configure docker to use the GCR credentials.
        4. Run the containerized workload with the specified image, command, and arguments.
        """
        arg = (" ").join(self.container_args or [])

        gcs_files = (" ").join([f'"{w}"' for w in self.container_files])
        dest_paths = (" ").join([f'"{w}"' for w in self.container_files.values()])

        init_work_disk = (
            dedent("""
                mkfs.ext4 -F /dev/disk/by-id/google-work-disk
                mkdir -p /mnt/disks/work
                mount -o discard,defaults /dev/disk/by-id/google-work-disk /mnt/disks/work
                chmod a+w /mnt/disks/work
            """)
            if self.work_disk_size_gb
            else ""
        )

        return dedent(f"""
            #!/bin/bash
            set -v
            {init_work_disk}
            useradd -m app
            usermod -a -G docker app
            mkdir -p /home/app
            cd /home/app
            orig=( {gcs_files} )
            dest=( {dest_paths} )
            if [ -n "${{dest[*]}}" ]; then
                for d in "${{!dest[@]}}"; do
                    if=${{orig[$d]}}
                    od=$(dirname "${{dest[$d]}}")
                    mkdir -p "$od"
                    of=${{dest[$d]}}
                    echo "Copying $if to $of ($od)"
                    sudo -u app docker run \
                        -v /home/app/"$od":/downloads/"$od" \
                        --entrypoint gsutil \
                        voyz/gsutil_wrap:latest \
                        cp "$if" /downloads/"$of"
                done
            fi
            sudo -u app docker-credential-gcr configure-docker --registries europe-west1-docker.pkg.dev
            sudo -u app docker run \\
                --name {CONTAINER_NAME} \\
                {self.build_env_params()}
                {self.build_volume_params()}
                --log-driver="gcplogs" \\
                {self.container_image} \\
                {self.container_command} {arg}
        """)

    def declare_instance(self) -> compute_v1.Instance:
        """Declare the instance to be created.

        The instance includes:

        - A COS image as the boot disk.
        - A work disk if `work_disk_size_gb` is set, otherwise just the 10GB default boot disk.
        - Labels for the instance.
        - The startup script.
        - Network configuration.
        - Service account and scopes.
        """
        boot_disk = compute_v1.AttachedDisk(
            auto_delete=True,
            boot=True,
            initialize_params=compute_v1.AttachedDiskInitializeParams(
                disk_type=f"zones/{self.zone}/diskTypes/pd-ssd",
                labels=self.labels,
                source_image="projects/cos-cloud/global/images/cos-113-18244-151-50",
            ),
        )

        work_disk = compute_v1.AttachedDisk(
            auto_delete=True,
            device_name="work-disk",
            initialize_params=compute_v1.AttachedDiskInitializeParams(
                disk_size_gb=self.work_disk_size_gb,
                labels=self.labels,
                disk_type=f"zones/{self.zone}/diskTypes/pd-ssd",
            ),
        )

        disks = [boot_disk, work_disk] if self.work_disk_size_gb else [boot_disk]

        # Decide which service account to use. To honor the impersonation chain,
        # we need to get the last service account in the chain. If there is nothing,
        # we use the default service account.
        service_account_email = "default"
        try:
            service_account_email = self.hook._get_credentials_email
        except Exception:
            self.log.warning("Failed to get the service account email from the credentials.")
        self.log.info(f"using service account {service_account_email} for the instance creation")
        if self.impersonation_chain:
            if isinstance(self.impersonation_chain, str):
                service_account_email = self.impersonation_chain.split(",")[-1]
            elif isinstance(self.impersonation_chain, Sequence):
                service_account_email = self.impersonation_chain[-1]
            self.log.info(f"using service account {service_account_email} from the impersonation chain")

        return compute_v1.Instance(
            name=self.instance_name,
            description="unified pipeline runner instance",
            machine_type=f"zones/{self.zone}/machineTypes/{self.machine_type}",
            disks=disks,
            labels=self.labels,
            metadata=types.Metadata(
                items=[
                    {
                        "key": "google-logging-enabled",
                        "value": "true",
                    },
                    {
                        "key": "google-monitoring-enabled",
                        "value": "true",
                    },
                    {
                        "key": "startup-script",
                        "value": self.startup_script(),
                    },
                ]
            ),
            network_interfaces=[
                types.NetworkInterface(
                    access_configs=[
                        types.AccessConfig(
                            name="External NAT",
                            network_tier="PREMIUM",
                        ),
                    ]
                )
            ],
            service_accounts=[
                types.ServiceAccount(
                    email=service_account_email,
                    scopes=([
                        "https://www.googleapis.com/auth/cloud-platform",
                        "https://www.googleapis.com/auth/devstorage.full_control",
                        "https://www.googleapis.com/auth/logging.write",
                        "https://www.googleapis.com/auth/monitoring.write",
                        "https://www.googleapis.com/auth/servicecontrol",
                        "https://www.googleapis.com/auth/service.management.readonly",
                        "https://www.googleapis.com/auth/trace.append",
                        *self.container_scopes,
                    ]),
                ),
            ],
        )

    def start(self):
        """Create a Google Compute Engine instance and run a containerized workload on it."""
        self.client = compute_v1.InstancesClient()
        i = self.declare_instance()

        try:
            operation = self.client.insert(
                project=self.project,
                zone=self.zone,
                instance_resource=i,
            )
            wait_for_extended_operation(
                operation,
                verbose_name="instance insertion",
                timeout=int(self.execution_timeout.total_seconds()) if self.execution_timeout else None,
                log=self.log,
            )
        except Exception as e:
            raise AirflowException(f"Failed to create instance {self.instance_name}") from e

        self.log.info(f"created vm {self.instance_name}")

    def copy_machine_logs(self) -> None:
        """Copy logs from the machine to the Airflow logs."""
        client = self.logging_hook.get_conn()
        query = f'resource.type="gce_instance" jsonPayload.instance.name="{self.instance_name}" jsonPayload.container.name="/{CONTAINER_NAME}"'
        entries = client.list_entries(
            filter_=query,
            order_by=google_logging.ASCENDING,
            page_size=1000,
        )
        for entry in entries:
            self.log.info(entry.payload.get("message", "Empty log message"))

    def poke(self, context: Context) -> bool:
        """Check if the instance is still running in a synchronous way."""
        # We must implement this if we want to run this sensor in a non-deferrable mode.
        return False

    def execute(self, context: Context):
        """Set up and execute the sensor, then start the trigger."""
        # Try to extract the impersonation chain and project from the connection
        conn = self.hook.get_connection(self.gcp_conn_id)
        impersonation_chain = conn.extra_dejson.get("impersonation_chain")
        if impersonation_chain:
            self.log.info(f"setting impersonation_chain from connection: {impersonation_chain}")
            self.impersonation_chain = impersonation_chain

        dag_run = context.get("dag_run")
        if dag_run:
            default_run_label = dag_run.run_id
        run_label = context.get("params", {}).get("run_label", default_run_label)
        self.labels["run"] = run_label
        self.start()

        if not self.deferrable:
            super().execute(context)
        elif not self.poke(context):
            self.log.info("Deferring the sensor execution.")
            self.defer(
                timeout=self.execution_timeout,
                trigger=ComputeEngineExitCodeTrigger(
                    instance_name=self.instance_name,
                    project=self.project,
                    zone=self.zone,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    poll_sleep=self.poll_interval,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, str | list]) -> bool:
        """Continue task execution after the sensor has triggered.

        Returns True if the trigger returns an event with the success status, otherwise raises
        an exception.
        """
        self.copy_machine_logs()
        if event["status"] == "success":
            self.log.info(event["message"])
            return True
        raise AirflowException(f"Sensor failed: {event['message']}")

    @cached_property
    def hook(self) -> ComputeEngineHook:
        """Return the Google Compute Engine hook."""
        return ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    @cached_property
    def logging_hook(self) -> CloudLoggingHook:
        """Return the Google Cloud Logging hook."""
        return CloudLoggingHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )


class ComputeEngineExitCodeTrigger(BaseTrigger):
    """Trigger that checks for the exit code of a google compute engine instance startup script.

    Args:
        instance_name: Name of the instance to check.
        project_id: Optional, the Google Cloud project ID where the job is.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        zone: The zone of the VM (for example europe-west1-b).
        gcp_conn_id: The connection ID to use when connecting to Google Cloud.
        impersonation_chain: Optional service account or chain to impersonate.
        poll_sleep: Time (seconds) to wait between two consecutive checks.
    """

    def __init__(
        self,
        instance_name: str,
        project: str,
        zone: str,
        gcp_conn_id: str,
        impersonation_chain: str | Sequence[str] | None,
        poll_sleep: int,
    ) -> None:
        super().__init__()
        self.instance_name = instance_name
        self.project = project
        self.zone = zone
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.poll_sleep = poll_sleep
        self.start_time = datetime.datetime.now(datetime.UTC)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "orchestration.operators.gce.ComputeEngineExitCodeTrigger",
            {
                "instance_name": self.instance_name,
                "project": self.project,
                "zone": self.zone,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "poll_sleep": self.poll_sleep,
            },
        )

    async def run(self):
        """Loop until the vm startup script exits.

        Yields a TriggerEvent with success status if the exit code is 0.

        Yields a TriggerEvent with error status in any other exit code or if
        any exception is raised while looping.

        In any other case the Trigger will wait for a specified amount of time
        stored in self.poll_sleep variable.
        """
        exit_code = None
        try:
            while True:
                exit_code = await self.hook.get_exit_code(
                    self.project,
                    self.instance_name,
                    self.start_time,
                )

                self.log.info(f"VM {self.instance_name} exit code is {exit_code}")

                if exit_code == 0:
                    yield TriggerEvent({
                        "status": "success",
                        "message": f"VM {self.instance_name} exit code is {exit_code}",
                    })
                    return
                elif exit_code is not None:
                    yield TriggerEvent({
                        "status": "error",
                        "message": f"VM {self.instance_name} exit code is {exit_code}",
                    })
                    return
                self.log.info("VM startup script is still running.")
                await asyncio.sleep(self.poll_sleep)
        except Exception as e:
            self.log.error("Error occurred while checking startup script exit code.")
            yield TriggerEvent({"status": "error", "message": f"{type(e)}: {e!r}"})

    @cached_property
    def hook(self) -> CloudLoggingAsyncHook:
        """Return the Google Cloud Logging async hook."""
        return CloudLoggingAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )


class DeleteInstanceOperator(ComputeEngineDeleteInstanceOperator):
    def __init__(
        self,
        *,
        zone: str = GCP_ZONE,
        project_id: str = GCP_PROJECT_PLATFORM,
        **kwargs,
    ) -> None:
        super().__init__(zone=zone, project_id=project_id, **kwargs)

    def execute(self, context: Context) -> None:
        super().execute(context)

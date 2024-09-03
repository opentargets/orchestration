"""Custom sensor that runs a containerized workload on a Google Compute Engine instance."""

import asyncio
import datetime
import logging
import time
from functools import cached_property
from textwrap import dedent
from typing import Any, Sequence

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.compute import ComputeEngineHook
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
from google.cloud.logging_v2.services.logging_service_v2 import (
    LoggingServiceV2AsyncClient,
)

from ot_orchestration.utils.common import (
    GCP_PROJECT_PLATFORM,
    GCP_REGION,
    prepare_labels,
)

CONTAINER_NAME = "workload_container"
LOGGING_REQUEST_INTERVAL = 5


def wait_for_extended_operation(
    operation: ExtendedOperation,
    verbose_name: str = "operation",
    timeout: int = 300,
    log: logging.Logger = logging.getLogger(__name__),
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
        timeout: how long (in seconds) to wait for operation to finish.
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
                break
            except ResourceExhausted:
                self.log.warning(
                    "Rate limit for logging api exceeded, waiting for %d seconds",
                    self.request_interval,
                )
                time.sleep(self.request_interval)
                self.request_interval *= 2

        return entries


class CloudLoggingHook(GoogleBaseHook):
    """Hook for the Google Logging service.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
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
                credentials=self.get_credentials(),
                client_info=CLIENT_INFO,
            )
        return self._client


class CloudLoggingAsyncHook(GoogleBaseHook):
    """Async hook for the Google Logging service.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
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
        initial_timestamp: datetime.datetime,
    ) -> int:
        """Get the exit code of the startup script of a Google Compute Engine instance.

        According to Google Cloud documentation in `viewing the output of a Linux startup script
        <https://cloud.google.com/compute/docs/instances/startup-scripts/linux#viewing-output>`__,

        The exit code of the startup script should be visible in serial port 1 or the system logs,
        but it is not there. The only way to get the exit code is to search the logs for the message
        by using the logging API.

        The exit code is the number that follows the string "exit status" in the message. To harden
        the query, we are using the regex:

        startup-script[\w\":\s]*exit status [0-9]+

        This is because the message changes depending on the script exiting successfully:

        startup-script exit status 0

        And when the script fails:

        Script "startup-script" failed with error: exit status 1
        """  # noqa: D301
        client = self.get_conn()
        timestamp_str = initial_timestamp.isoformat()
        query = f'resource.type="gce_instance" labels.instance_name="{instance_name}" timestamp>"{timestamp_str}" jsonPayload.message=~"startup-script[\w\\\":\s]*exit status [0-9]+"'  # fmt: skip
        log_pages = None

        while True:
            try:
                log_pages = await client.list_log_entries(
                    resource_names=[f"projects/{project_name}"],
                    filter=query,
                    timeout=300,
                )
                break
            except ResourceExhausted:
                self.log.warning(
                    "Rate limit for logging api exceeded, waiting for %d seconds",
                    self.request_interval,
                )
                await asyncio.sleep(self.request_interval)
                self.request_interval *= 2
            except RetryError as e:
                self.log.warning(
                    "Error occurred while fetching log entries: %s, retrying after %d seconds",
                    e,
                    self.request_interval,
                )
                await asyncio.sleep(self.request_interval)
                self.request_interval *= 2

        logs = None
        try:
            logs = await anext(log_pages.pages, None)
        except Exception as e:
            self.log.error("Error occurred while fetching log entries: %s", e)

        if logs and logs.entries:
            entry = logs.entries[0]
            return int(entry.json_payload["message"].split("exit status", 1)[1].strip())

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

    :param project_id: Optional, the Google Cloud project ID where the job is.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param zone: The zone where the instance will be created (default is f'{GCP_REGION}-b').
    :param instance_name: Name of the instance name that will run the workload.
    :param labels: Optional dict of labels to apply to the instance on top of the default ones, which
        are `team: open-targets`, `product: platform`, `environment: development` or `production`,
        and `created_by: unified-orchestrator`. Refer to `controlled vocabularies
        <https://github.com/opentargets/controlled-vocabularies/blob/main/infrastructure.yaml>`__ for
        more information.
    :param container_image: Container image to run.
    :param container_command: Command to run inside the container (optional).
    :param container_args: Arguments to pass to the container (optional).
    :param container_env: Environment variables to pass to the container (optional).
    :param container_service_account: Service account to use for the instance (default default).
    :param container_scopes: A list of extra scopes to add to the service account if any are needed.
    :param container_files: Files to copy to the instance (optional). This is a dictionary where
        the key is a GCS path in the form `gs://bucket/path/to/file` and the value is the path
        where the file will be copied to in the instance. This is intented for small files needed
        to run the workload (like the configuration). Large files should be downloaded by the
        workload itself. The paths specified as values will be relative to the `/home/app` directory
        and will be created if they don't exist. Inside the docker container, they will be mounted
        under the root directory.
    :param machine_type: Machine type to use for the instance (default e2-standard-2).
    :param work_disk_size_gb: If present, a second disk with the specified size in GB will be
        attached to the instance besides the boot disk, to be used by the workload. The disk will
        be formatted with ext4 and mounted under `/mnt/disks/work`. The instance will have write
        permissions to the disk. The disk will be deleted when the instance is deleted.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: If True, run the sensor in deferrable mode.
    :param poll_interval: Time (seconds) to wait between checks for the job status.
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
        zone: str = f"{GCP_REGION}-b",
        instance_name: str,
        labels: dict[str, str] = {},
        container_image: str,
        container_command: str = "",
        container_args: list[str] | None = None,
        container_env: dict[str, str] | None = None,
        container_service_account: str = "default",
        container_scopes: list[str] = [],
        container_files: dict[str, str] | None = None,
        machine_type: str = "c3d-standard-8",
        work_disk_size_gb: int = 0,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        poll_interval: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project = project
        self.zone = zone
        self.instance_name = instance_name
        self.labels = labels
        self.container_image = container_image
        self.container_command = container_command
        self.container_args = container_args
        self.container_env = container_env
        self.container_service_account = container_service_account
        self.container_scopes = container_scopes
        self.container_files = container_files
        self.machine_type = machine_type
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.work_disk_size_gb = work_disk_size_gb
        self.deferrable = deferrable
        self.poll_interval = poll_interval

    def build_env_params(self):
        """Build the environment parameters for the docker run command."""
        return ("\n").join(
            [f"    -e {k}={v} \\" for k, v in self.container_env.items()]
        )

    def build_volume_params(self):
        """Build the volume parameters for the docker run command."""
        vs = [f"    -v /home/app/{p}:{p} \\" for p in self.container_files.values()]
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

        gcs_files = (" ").join([f'"{w}"' for w in self.container_files.keys()])
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
                for i in "${{!dest[@]}}"; do
                    if=${{orig[$i]}}
                    od=$(dirname "${{dest[$i]}}")
                    if [ "$od" = "." ]; then
                        od=""
                    else
                        mkdir -p "$od"
                    fi
                    of=${{dest[$i]}}
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

    def declare_instance(self) -> compute_v1.InstanceTemplate:
        """Declare the instance to be created.

        The instance includes:

        - A COS image as the boot disk.
        - A work disk if `work_disk_size_gb` is set, otherwise just the 10GB default boot disk.
        - Labels for the instance.
        - The startup script.
        - Network configuration.
        - Service account and scopes.
        """
        labels = prepare_labels(self.labels, self.project)

        boot_disk = compute_v1.AttachedDisk(
            auto_delete=True,
            boot=True,
            initialize_params=compute_v1.AttachedDiskInitializeParams(
                disk_type=f"zones/{self.zone}/diskTypes/pd-ssd",
                labels=labels,
                source_image="projects/cos-cloud/global/images/cos-stable-113-18244-151-9",
            ),
        )

        work_disk = compute_v1.AttachedDisk(
            auto_delete=True,
            device_name="work-disk",
            initialize_params=compute_v1.AttachedDiskInitializeParams(
                disk_size_gb=self.work_disk_size_gb,
                labels=labels,
                disk_type=f"zones/{self.zone}/diskTypes/pd-ssd",
            ),
        )

        disks = [boot_disk, work_disk] if self.work_disk_size_gb else [boot_disk]

        return compute_v1.Instance(
            name=self.instance_name,
            description="unified orchestrator runner instance",
            machine_type=f"zones/{self.zone}/machineTypes/{self.machine_type}",
            disks=disks,
            labels=labels,
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
                    email=self.container_service_account or "default",
                    scopes=(
                        [
                            "https://www.googleapis.com/auth/cloud-platform",
                            "https://www.googleapis.com/auth/devstorage.full_control",
                            "https://www.googleapis.com/auth/logging.write",
                            "https://www.googleapis.com/auth/monitoring.write",
                            "https://www.googleapis.com/auth/servicecontrol",
                            "https://www.googleapis.com/auth/service.management.readonly",
                            "https://www.googleapis.com/auth/trace.append",
                        ]
                        + self.container_scopes
                    ),
                ),
            ],
        )

    def start(self) -> int:
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
                timeout=self.execution_timeout,
                log=self.log,
            )
        except Exception as e:
            raise AirflowException(
                f"Failed to create instance {self.instance_name}"
            ) from e

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
            self.log.log(level=logging.INFO, msg=entry.payload.get("message", ""))

    def poke(self, context: Context) -> bool:
        """Check if the instance is still running in a synchronous way."""
        # We must implement this if we want to run this sensor in a non-deferrable mode.
        return False

    def execute(self, context: Context) -> bool:
        """Set up and execute the sensor, then start the trigger."""
        self.start()
        self.log.info("Instance created, now checking for the exit code.")

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

    def execute_complete(self, context: Context, event: dict[str, str | list]) -> None:
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

    :param instance_name: Name of the instance to check.
    :param project_id: Optional, the Google Cloud project ID where the job is.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param zone: The zone of the VM (for example europe-west1-b).
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param poll_sleep: Time (seconds) to wait between two consecutive checks.
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
        self.timestamp = datetime.datetime.now(datetime.timezone.utc)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "ot_orchestration.operators.gce.ComputeEngineExitCodeTrigger",
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
                    self.timestamp,
                )

                self.log.info(f"VM {self.instance_name} exit code is {exit_code}")

                if exit_code == 0:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": f"VM {self.instance_name} exit code is {exit_code}",
                        }
                    )
                    return
                elif exit_code is not None:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"VM {self.instance_name} exit code is {exit_code}",
                        }
                    )
                    return
                self.log.info("VM startup script is still running.")
                await asyncio.sleep(self.poll_sleep)
        except Exception as e:
            self.log.error("Error occurred while checking startup script exit code.")
            yield TriggerEvent({"status": "error", "message": f"{type(e)}: {str(e)}"})

    @cached_property
    def hook(self) -> CloudLoggingAsyncHook:
        """Return the Google Cloud Logging async hook."""
        return CloudLoggingAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

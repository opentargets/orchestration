"""Instance specification for Google Batch tasks."""

from __future__ import annotations

from functools import cached_property
from typing import Annotated, ClassVar, Literal

from google.cloud import batch_v1
from pydantic import BaseModel, StringConstraints, model_validator

from orchestration.utils.common import GCP_REGION
from orchestration.utils.labels import Labels


class InstanceSpec(BaseModel):
    """Machine instance specification for Google Batch tasks.

    This class maps to the :py:class:`google.cloud.batch_v1.AllocationPolicy.InstancePolicy` class used in Google Batch job definitions.

    Attributes:
        machine_type: Type of machine to be used for the batch task. Must be n*-[standard|highmem|highcpu]-* (e.g., n1-standard-4, n2-highmem-8, etc.).
        provisioning_model: Provisioning model for the batch task. Must be one of `STANDARD`, `SPOT`, or `PREEMPTIBLE`. The default value is `SPOT`.
    """

    machine_type: Annotated[str, StringConstraints(pattern=r"^n\d-(standard|highmem|highcpu)-\d+$")] = "n1-standard-4"
    """Type of machine to be used for the batch task. Must be n*-[standard|highmem|highcpu]-* (e.g., n1-standard-4, n2-highmem-8, etc.)."""

    provisioning_model: Literal["STANDARD", "SPOT", "PREEMPTIBLE"] = "SPOT"
    """Provisioning model for the batch task. Must be one of `STANDARD`, `SPOT`, or `PREEMPTIBLE`. The default value is `SPOT`."""

    def build(self) -> batch_v1.AllocationPolicy.InstancePolicy:
        """Build a `google.cloud.batch_v1.AllocationPolicy.InstancePolicy` object from the instance specification.

        Returns:
            batch_v1.AllocationPolicy.InstancePolicy: The built InstancePolicy object.

        Example:
        ---
        >>> spec = InstanceSpec()
        >>> policy = spec.build()
        >>> isinstance(policy, batch_v1.AllocationPolicy.InstancePolicy)
        True
        >>> policy.machine_type
        'n1-standard-4'
        >>> policy.provisioning_model == batch_v1.AllocationPolicy.ProvisioningModel.SPOT
        True
        >>> InstanceSpec(machine_type="n2-highmem-8", provisioning_model="STANDARD").build().machine_type
        'n2-highmem-8'
        """
        return batch_v1.AllocationPolicy.InstancePolicy(
            machine_type=self.machine_type,
            provisioning_model=batch_v1.AllocationPolicy.ProvisioningModel[self.provisioning_model],
        )


class InstanceResourceSpec(BaseModel):
    """Resource specification for a single instance.

    Attributes:
        cpu_milli: CPU resources in milli-CPUs. 1000 = 1 full CPU, 2000 = 2 CPUs, etc.
        memory_mib: Memory in MiB (mebibytes). 4096 MiB = 4 GiB.
        boot_disk_mib: Boot disk size in MiB (mebibytes). 102400 MiB = 100 GiB.

    Note:
        Google Batch expects MiB (base-2), not MB (base-10):

        | Unit | Base    | Calculation | Bytes     |
        |------|---------|-------------|-----------|
        | MiB  | Binary  | 2^20        | 1,048,576 |
        | MB   | Decimal | 10^6        | 1,000,000 |
    """

    cpu_milli: int
    """Size of CPU resources in milli-CPUs. For example, 1000 = 1 full CPU, 2000 = 2 CPUs, etc."""

    memory_mib: int
    """Size of memory in MiB (mebibytes). For example, 4096 MiB = 4 GiB."""

    boot_disk_mib: int
    """Size of boot disk in MiB (mebibytes). For example, 102400 MiB = 100 GiB."""

    def build(self) -> batch_v1.ComputeResource:
        """Build a `google.cloud.batch_v1.ComputeResource` object from the instance resource specification.

        Returns:
            batch_v1.ComputeResource: The built ComputeResource object.

        Example:
        ---
        >>> spec = InstanceResourceSpec(cpu_milli=2000, memory_mib=4096, boot_disk_mib=102400)
        >>> cr = spec.build()
        >>> isinstance(cr, batch_v1.ComputeResource)
        True
        >>> cr.cpu_milli
        2000
        >>> cr.memory_mib
        4096
        >>> cr.boot_disk_mib
        102400
        """
        return batch_v1.ComputeResource(
            cpu_milli=self.cpu_milli,
            memory_mib=self.memory_mib,
            boot_disk_mib=self.boot_disk_mib,
        )


class AllocationSpec(BaseModel):
    """Allocation specification for Google Batch tasks."""

    DEFAULT_REGION: ClassVar[str] = f"regions/{GCP_REGION}"

    instance: InstanceSpec
    """Instance specification for the batch task."""

    service_account_email: str | None = None
    """Service account email to be used for the batch task."""

    service_account_scopes: list[str] | None = None
    """Service account scopes to be used for the batch task."""

    region: Annotated[str | None, StringConstraints(pattern=r"^regions/[a-z0-9-]+$")] = None
    """Region specification for the batch task. In format regions/<region>."""

    zones: list[Annotated[str, StringConstraints(pattern=r"^zones/[a-z0-9-]+$")]] | None = None
    """Zone specification for the batch task. In format [zones/<zone>]. Can be multiple zones."""

    labels: dict[str, str] | None = None
    """Labels to be applied to each instance in the task group."""

    @model_validator(mode="after")
    def validate_region_or_zone(self) -> AllocationSpec:
        """Validate that either region or zones are specified, but not both.

        Args:
            allocation_spec (AllocationSpec): The allocation specification to validate.

        Returns:
            AllocationSpec: The validated allocation specification.

        Raises:
            ValueError: If both region and zones are specified, or if neither is specified.

        Example:
        ---
        >>> AllocationSpec(instance=InstanceSpec()).region
        'regions/europe-west1'
        >>> AllocationSpec(instance=InstanceSpec(), zones=["zones/europe-west1-b"]).region is None
        True
        >>> AllocationSpec(instance=InstanceSpec(), region="regions/europe-west1", zones=["zones/europe-west1-b"])  # doctest: +ELLIPSIS
        Traceback (most recent call last):
            ...
        pydantic_core._pydantic_core.ValidationError: ...
        """
        if self.region and self.zones:
            raise ValueError("Either `region` or `zones` can be specified, but not both.")
        if self.zones:
            return self
        # By default set the region to the default region if neither region nor zones are specified
        self.region = self.DEFAULT_REGION
        return self

    @cached_property
    def service_account(self) -> batch_v1.ServiceAccount:
        """Get the service account for the batch task based on the specified email and scopes.

        Returns:
            batch_v1.ServiceAccount: The service account for the batch task.
        """
        return batch_v1.ServiceAccount(
            email=self.service_account_email,
            scopes=self.service_account_scopes or [],
        )

    @cached_property
    def location(self) -> batch_v1.AllocationPolicy.LocationPolicy:
        """Get the location policy for the batch task based on the specified region or zones.

        Returns:
            batch_v1.AllocationPolicy.LocationPolicy: The location policy for the batch task.
        """
        return batch_v1.AllocationPolicy.LocationPolicy(
            allowed_locations=[self.region] if self.region else self.zones
        )

    def build(self) -> batch_v1.AllocationPolicy:
        """Build a `google.cloud.batch_v1.AllocationPolicy` object from the allocation specification.

        Returns:
            batch_v1.AllocationPolicy: The built AllocationPolicy object.

        Example:
        ---
        >>> alloc = AllocationSpec(instance=InstanceSpec(), labels={"team": "test"})
        >>> ap = alloc.build()
        >>> isinstance(ap, batch_v1.AllocationPolicy)
        True
        >>> list(ap.location.allowed_locations)
        ['regions/europe-west1']
        >>> ap.instances[0].policy.machine_type
        'n1-standard-4'
        >>> alloc_zones = AllocationSpec(instance=InstanceSpec(), zones=["zones/europe-west1-b"], labels={"team": "test"})
        >>> list(alloc_zones.build().location.allowed_locations)
        ['zones/europe-west1-b']
        """
        ap = {
            "instances": [batch_v1.AllocationPolicy.InstancePolicyOrTemplate(policy=self.instance.build())],
            "labels": self.labels or dict(Labels()),
        }
        if self.location:
            ap["location"] = self.location
        if self.service_account_email:
            ap["service_account"] = self.service_account

        return batch_v1.AllocationPolicy(**ap)

"""Configuration class for the unified pipeline."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from orchestration.dags.config.app_config import AppConfig
from orchestration.operators.dataproc import ClusterDefinition
from orchestration.utils.common import GCP_PROJECT_PLATFORM

if TYPE_CHECKING:
    from typing import Any


class UnifiedPipelineConfig:
    """Configuration class for the Unified Pipeline.

    This class is used to provide the config for the Unified Pipeline and all the
    applications run by it: PIS, PTS, ETL and GENTROPY.

    The configuration is loaded, parsed and in the case of the application configs,
    templates are rendered with values from the pipeline configuration.

    There are hardcoded config values that are not meant to change often. If they
    become more dynamic, they can be moved to unified_pipeline.yaml.
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        config_path = Path(__file__).parent

        up = AppConfig.from_file(file_path=config_path / "unified_pipeline.yaml")
        self._steps = up.get("steps")

        self.run_name = up.get("run_name") or datetime.now().strftime("%Y%m%d-%H%M")
        """Used for labelling resources."""
        self.release_uri: str = f"gs://open-targets-pre-data-releases/{up.get('release_name')}"
        """The place where the production release files are read from and/or written to."""
        self.is_dev = up.get("is_dev", True)
        """Whether this is a development or production run."""
        self.dev_uri = f"gs://open-targets-pipeline-runs/{self.run_name}" if self.is_dev else None
        """The place where the development run files are read from and written to."""
        self.service_account_extra_scopes = ["https://www.googleapis.com/auth/drive"]
        """Extra scopes to be added to the service account in executor machines"""
        """- the drive scope is needed to download Google Drive spreadsheets for the pis_otar step"""
        self.is_ppp = up.get("is_ppp")
        """Whether this is a ppp run or public platform run."""
        self.num_partitions = 20
        """The default number of partitions for steps using spark that do not specify it."""

        data_sources_exclude = "[]" if self.is_ppp else '["ot_crispr", "encore", "ot_crispr_validation"]'

        self.pis = AppConfig.from_file(
            file_path=config_path / "pis.yaml",
            template_context={
                "release_uri": self.dev_uri or self.release_uri,
                "chembl_version": up.get("chembl_version"),
                "efo_version": up.get("efo_version"),
                "ensembl_version": up.get("ensembl_version"),
                "gencode_version": up.get("gencode_version"),
                "depmap_version": up.get("depmap_version"),
                "hpo_version": up.get("hpo_version"),
                "mondo_version": up.get("mondo_version"),
                "ot_curation": up.get("ot_curation"),
                "probes_drugs_version": up.get("probes_drugs_version"),
                "gnomad_version": up.get("gnomad_version"),
            },
        )
        """The internal configuration for PIS steps."""

        if self.is_ppp:
            self.pis = self.pis.overwrite(config_path / "ppp" / "pis.override.yaml")
        """The internal configuration for PIS steps, with PPP-specific overrides."""

        self.pts = AppConfig.from_file(
            file_path=config_path / "pts.yaml",
            template_context={
                "release_uri": self.dev_uri or self.release_uri,
                "release_name": up.get("release_name"),
            },
        )
        """The internal configuration for PTS steps."""

        if self.is_ppp:
            self.pts = self.pts.overwrite(config_path / "ppp" / "pts.override.yaml")
        """The internal configuration for PTS steps, with PPP-specific overrides."""

        self.etl = AppConfig.from_file(
            file_path=config_path / "etl.conf",
            template_context={
                "release_uri": self.dev_uri or self.release_uri,
                "data_sources_exclude": data_sources_exclude,
            },
        )
        """The internal configuration for ETL steps."""

        if self.is_ppp:
            self.etl = self.etl.overwrite(config_path / "ppp" / "etl.overrides.conf")
        """The internal configuration for ETL steps, with PPP-specific overrides."""

        self.gentropy = AppConfig.from_file(
            file_path=config_path / "gentropy.yaml",
            template_context={
                "release_uri": self.dev_uri or self.release_uri,
                "gentropy_version": up.get("gentropy_version"),
                "l2g_training_version": up.get("release_name"),
                "vep_version": up.get("vep_version"),
            },
        )
        """The internal configuration for GENTROPY steps."""

        if self.is_ppp:
            self.gentropy = self.gentropy.overwrite(config_path / "ppp" / "gentropy.overrides.yaml")
        """The internal configuration for GENTROPY steps, with PPP-specific overrides."""

        self.clusters = AppConfig.from_file(
            file_path=config_path / "clusters.yaml",
            template_context={
                "pts_version": up.get("pts_version"),
                "gentropy_version": up.get("gentropy_version"),
                "requester_pays_project_id": GCP_PROJECT_PLATFORM,
            },
        )
        """The cluster definitions."""

        # PIS-specific settings.
        pis_image = "europe-west1-docker.pkg.dev/open-targets-eu-dev/pis/pis"
        pis_version = up.get("pis_version")
        self.pis_image = f"{pis_image}:{pis_version}"
        """The image and tag used to run PIS steps."""
        self.pis_disk_size = 150
        """The disk size for PIS vms, in GB.
            Note: although not all steps need this much space, it is simpler to
                have a single value for all steps, the machines are short-lived
                and it doesn't matter with respect to cost.
        """

        # PTS-specific settings.
        pts_image = "europe-west1-docker.pkg.dev/open-targets-eu-dev/pts/pts"
        pts_version = up.get("pts_version")
        self.pts_image = f"{pts_image}:{pts_version}"
        """The image and tag used to run PTS steps."""
        self.pts_machine_type = "n1-standard-32"
        """The machine type used to run PTS steps."""
        self.pts_disk_size = 300
        """The disk size for PTS vms, in GB."""

        # ETL-specific settings.
        etl_version = up.get("etl_version")
        self.etl_jar_origin_uri = f"gs://opentargets-pipelines/up/etl/etl-{etl_version}.jar"
        """The URI where the jar used to run ETL is fetched from."""

        # GENTROPY-specific settings.
        self.gentropy_main_python_file_uri = "gs://genetics_etl_python_playground/initialisation/cli.py"
        self.gentropy_cluster_init_script_uri = (
            "gs://genetics_etl_python_playground/initialisation/install_dependencies_on_cluster.sh"
        )

    def pis_env_vars(self, step_name: str) -> dict[str, str]:
        """Return the environment variables for a PIS step."""
        return {
            "PIS_STEP": step_name.removeprefix("pis_"),
            "PIS_CONFIG_PATH": "/config.yaml",
        }

    def pts_env_vars(self, step_name: str) -> dict[str, str]:
        """Return the environment variables for a PTS step."""
        return {
            "PTS_STEP": step_name.removeprefix("pts_"),
            "PTS_CONFIG_PATH": "/config.yaml",
        }

    def steps(self, prefix: str = "") -> list[str]:
        """Return a list of steps in the pipeline.

        Args:
            prefix (str): Filter steps by prefix, to get the list of steps for a
                specific stage. For example, `pis_` for PIS steps.
            ppp (bool): Whether to include PPP-exclusive steps. Defaults to `False`.

        Returns:
            list[str]: The list of step names.
        """
        relevant_steps: list[str] = []

        stage_steps = {k: v for k, v in self._steps.items() if k.startswith(prefix)}
        for step_name, step_deps in stage_steps.items():
            if step_deps and step_deps.get("ppp_only") and not self.is_ppp:
                continue
            relevant_steps.append(step_name)
        return relevant_steps

    def step_config(self, step_name: str) -> dict[str, Any]:
        """Return the configuration for a step.

        This method gathers both the common configuration for the application and
        the specific configuration for the step. That specific configuration will
        therefore be nested under `steps.{step_name}` in the returned dict. See
        `step_specific_config` to get the specific configuration.

        Args:
            step_name (str): The name of the step, in the form `{stage}_{step_name}`.

        Returns:
            dict: The configuration for the step.
        """
        stage, step = step_name.split("_", 1)
        stage_config: AppConfig = getattr(self, stage)

        return {
            **stage_config.config,
            "steps": {step: stage_config.config.get("steps", {}).get(step, {})},
        }

    def step_specific_config(self, step_name: str) -> dict[str, Any]:
        """Return the specific configuration for a step.

        This method returns the specific step configuration, that is, only the
        keys under `steps.{step_name}` in the configuration file.

        Args:
            step_name (str): The name of the step, in the form `{stage}_{step_name}`.

        Returns:
            dict[str, Any]: The specific configuration for the step.
        """
        _, step = step_name.split("_", 1)
        return self.step_config(step_name).get("steps", {}).get(step, {})

    def step_cluster_definition(self, step_name: str) -> ClusterDefinition | None:
        """Return the cluster type and configuration for a step.

        This method finds the proper cluster definition by matching on the most
        specific cluster name that is a prefix of the step name. So if the step
        name is `pis_foo_bar`, and the cluster names are `pis_foo_` and `pis_`,
        the cluster definition for `pis_foo_` will be returned.

        A step can also be configured to not use a cluster by setting the
        `cluster` key to `False` in the step configuration. In this case, the
        method will return None. This is useful for Gentropy steps that are run
        using Google Batch.

        Args:
            step_name (str): The name of the step, in the form `{stage}_{step_name}`.

        Returns:
            ClusterDefinition | None: A ClusterDefinition object containing the
                cluster type and configuration for the step. If the step requires
                no cluster, returns None.

        Raises:
            ValueError: If no cluster definition is found for the step name.
        """
        if self.step_specific_config(step_name).get("cluster", True) is False:
            return None

        clusters = self.clusters.config.get("clusters", {})
        sorted_cluster_names = sorted(clusters.keys(), key=len, reverse=True)
        for cluster_name in sorted_cluster_names:
            if step_name.startswith(cluster_name):
                return ClusterDefinition(cluster_name, clusters[cluster_name])
        raise ValueError(f"No cluster definition found for step {step_name}.")

    def step_job_properties(self, step_name: str) -> dict[str, str]:
        """Return the spark job properties for a step.

        This method finds the proper job properties by matching on the most
        specific key in the `step_job_properties` dictionary that is a prefix of
        the step name. So if the step name is `pis_foo_bar`, and the keys are
        `pis_foo_` and `pis_`, the job properties for `pis_foo_` will be
        returned.

        Args:
            step_name (str): The name of the step, in the form `{stage}_{step_name}`.

        Returns:
            dict[str, str]: The spark job properties for the step. If none are
                found, an empty dictionary is returned.
        """
        property_dicts = self.clusters.config.get("step_job_properties", {})
        sorted_property_dicts = sorted(property_dicts.keys(), key=len, reverse=True)
        for property_dict in sorted_property_dicts:
            if step_name.startswith(property_dict):
                return property_dicts[property_dict]
        return {}

    def step_definition(self, step_name: str) -> dict[str, Any]:
        """Return the definition of a step.

        This method returns the step definition, which includes the step name,
        dependencies, and the number of partitions if it is a spark step.

        This is a good candidate for a refactor once a Step class is modeled in.

        Args:
            step_name (str): The name of the step, in the form `{stage}_{step_name}`.

        Returns:
            dict[str, Any]: The definition of the step.
        """
        definition = self._steps.get(step_name)
        # can't put the default in the get, as the content can actually be None
        # and that will not be replaced by the default
        return definition or {}

    def config_uri(self, step_name: str) -> str:
        """Return the URI of the configuration file for a step.

        Args:
            step_name (str): The name of the step, in the form `{stage}_{step_name}`.

        Returns:
            str: The URI of the configuration file for the step.
        """
        exts = {  # file extensions for any stage that does not use a yaml config
            "etl": "conf",
        }
        stage, _ = step_name.split("_", 1)
        ext = exts.get(stage, "yaml")
        return f"{self.dev_uri or self.release_uri}/etc/config/{step_name}.{ext}"

    def jar_uri(self, step_name: str) -> str:
        """Return the URI of the jar file used to run ETL.

        Args:
            step_name (str): The name of the step, in the form `{stage}_{step_name}`.

        Returns:
            str: The URI of the jar file.
        """
        _, step = step_name.split("_", 1)
        return f"{self.dev_uri or self.release_uri}/etc/bin/etl-{step}.jar"

    def manifest_uri(self) -> str:
        """Return the URI of the manifest file for the run.

        Returns:
            str: The URI of the manifest.
        """
        return f"{self.dev_uri or self.release_uri}/manifest.json"

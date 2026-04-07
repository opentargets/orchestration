"""Configuration class for the unified pipeline."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from orchestration.dags.config.app_config import AppConfig
from orchestration.models.environment import Environments
from orchestration.models.infrastructure import BatchJobRegistry, ClusterRegistry
from orchestration.models.pipeline.abc import PipelineConfig
from orchestration.utils.common import GCP_PROJECT_PLATFORM


class UnifiedPipelineConfig(PipelineConfig):
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
        """Logger for the unified pipeline configuration."""
        config_path = Path(__file__).parent

        up = AppConfig.from_file(file_path=config_path / "unified_pipeline.yaml")
        self._steps = up.get("steps")

        self.env = up.get("env", "Test")
        """Environment to run the pipeline in, e.g. "Prod", "Test", etc. The environment is used to derive the templating context to pre-fill the configuration."""
        self.environments = Environments(environments=up.get("environments", []))
        self.run_name = up.get("run_name") or datetime.now().strftime("%Y%m%d-%H%M")
        """Used for labelling resources."""
        self.release_uri: str = f"gs://open-targets-pre-data-releases/{up.get('release_name')}"
        """The place where the production release files are read from and/or written to."""
        self.is_dev = up.get("is_dev", True)
        """Whether this is a development or production run."""
        self.dev_uri = f"gs://opentargets-pipeline-runs/{self.run_name}" if self.is_dev else None
        """The place where the development run files are read from and written to."""
        self.service_account_extra_scopes = ["https://www.googleapis.com/auth/drive"]
        """Extra scopes to be added to the service account in executor machines"""
        """- the drive scope is needed to download Google Drive spreadsheets for the pis_otar step"""
        self.is_ppp = up.get("is_ppp")
        """Whether this is a ppp run or public platform run."""
        self.num_partitions = 20
        """The default number of partitions for steps using spark that do not specify it."""

        data_sources_exclude = "[]" if self.is_ppp else '["ot_crispr", "encore", "ot_crispr_validation"]'

        # INFRASTRUCTURE CONFIG
        self._clusters = AppConfig.from_file(
            file_path=config_path / "clusters.yaml",
            template_context={
                "pts_version": up.get("pts_version"),
                "gentropy_version": up.get("gentropy_version"),
                "requester_pays_project_id": GCP_PROJECT_PLATFORM,
            },
        )

        self._batch_jobs = AppConfig.from_file(
            file_path=config_path / "batch_jobs.yaml",
            template_context={},
        )
        if self.is_ppp:
            self.gentropy = self.gentropy.overwrite(config_path / "ppp" / "gentropy.overrides.yaml")
        """The internal configuration for GENTROPY steps, with PPP-specific overrides."""

        self.batch_jobs = BatchJobRegistry(items=self._batch_jobs.get("batch_jobs", {}))
        """The batch job definitions."""
        self.clusters = ClusterRegistry(
            items=self._clusters.get("clusters", {}),
            step_job_properties=self._clusters.get(
                "step_job_properties",
                {},
            ),
        )
        """The cluster definitions."""
        ## STAGE CONFIG

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

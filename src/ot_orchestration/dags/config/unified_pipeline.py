"""Configuration class for the unified pipeline."""

from pathlib import Path
from typing import Any

from ot_orchestration.utils import read_hocon_config, read_yaml_config


class UnifiedPipelineConfig:
    """Configuration class for the platform part of the unified pipeline.

    This class reads the configuration files for both the platform part of the
    unified pipeline dag as well as PIS and ETL applications, performs some
    operations on them and then exposes the values.

    Some fields in PIS and ETL application configuration files are replaced with
    values from the pipeline dag configuration, which is the only one the user of
    the orchestrator has to modify to run the unified pipeline.

    The configuration files are expected to be in the same directory as this file.
    They are:
    - `unified_pipeline.yaml`: contains the general configuration for the pipeline.
    - `pis.yaml`: contains the configuration for the PIS steps.
    - `etl.conf`: contains the configuration for the ETL steps.
    """

    def __init__(self) -> None:
        self.config_path = Path(__file__).parent / "unified_pipeline.yaml"
        self.pis_config_local_path = Path(__file__).parent / "pis.yaml"
        self.etl_config_local_path = Path(__file__).parent / "etl.conf"
        self.gentropy_config_local_path = Path(__file__).parent / "gentropy.yaml"

        # These are hardcoded config values that are not meant to change often.
        # It is safe to extract them from here into the config file in case they
        # become more dynamic. Just add them to self in the initialization so they
        # are available.

        # fmt: off
        # The service account and scopes to use (only used by PIS so far).
        # The drive scope is needed to download spreadsheets from Google Drive
        # for the PIS otar step.
        self.service_account = "platform-input-support@open-targets-eu-dev.iam.gserviceaccount.com" #fmt: skip
        self.service_account_scopes = ["https://www.googleapis.com/auth/drive"]

        # Pipeline settings.
        settings = read_yaml_config(self.config_path)
        release_name = settings["release_name"]
        self.release_uri:str = f'gs://open-targets-pre-data-releases/{release_name}'
        self.chembl_version = settings["chembl_version"]
        self.efo_version = settings["efo_version"]
        self.ensembl_version = settings["ensembl_version"]
        self.is_ppp = settings["is_ppp"]
        self.steps = settings["steps"]
        self.ppp_steps = [s for s, d in self.steps.items() if d and d.get('ppp_only', False)]

        # PIS-specific settings.
        pis_version = settings["pis_version"]
        self.pis_config = self.init_pis_config()
        # The base image for PIS, the version tag will be appended from the config file.
        pis_image_base = "europe-west1-docker.pkg.dev/open-targets-eu-dev/pis/pis"
        self.pis_image = f"{pis_image_base}:{pis_version}"
        self.pis_step_list = [s for s in settings["steps"].keys() if s.startswith("pis_")]
        self.pis_pool = 16  # number of parallel workers inside of each PIS step
        self.pis_disk_size = 150 # The disk size for PIS vms, in GB.
        # Note: although not all steps need this much space, it is easier to have a
        # single value for all steps, and the machines are so short-lived that it
        # doesn't matter much with respect to cost.

        # ONTOFORM-specific settings.
        ontoform_version = settings["ontoform_version"]
        self.ontoform_step_list = [s for s in settings["steps"].keys() if s.startswith("ontoform_")]
        self.ontoform_machine_type = 'n1-standard-32'
        # The base image for ONTOFORM, the version tag will be appended from the config file.
        ontoform_image_base = "europe-west1-docker.pkg.dev/open-targets-eu-dev/ontoform/ontoform"
        self.ontoform_image = f"{ontoform_image_base}:{ontoform_version}"

        # ETL-specific settings.
        etl_version = settings["etl_version"]
        self.etl_config = self.init_etl_config()
        self.etl_config_uri = f"{self.release_uri}/etc/config/etl.conf"
        # The base url for the ETL jar, the version will be replaced in from the config file.
        self.etl_jar_origin_uri = f"gs://opentargets-pipelines/up/etl/etl-{etl_version}.jar"
        self.etl_jar_uri = f"{self.release_uri}/etc/bin/etl.jar"  # fmt: skip
        self.etl_step_list = [s for s in settings["steps"].keys() if s.startswith("etl_")]

        # GENTROPY-specific settings.
        self.gentropy_version = settings["gentropy_version"]
        self.l2g_training = settings["l2g_training"]
        self.vep_version = settings["vep_version"]
        self.gentropy_config = self.init_gentropy_settings()
        self.gentropy_dataproc_cluster_settings = self.gentropy_config["dataproc_cluster_settings"]
        self.gentropy_step_list = [s for s in settings["steps"].keys() if s.startswith("gentropy_")]

    def pis_config_uri(self, step_name: str) -> str:
        """Return the google cloud url of the PIS configuration file for a step."""
        return f"{self.release_uri}/etc/config/{step_name}.yaml"

    def init_pis_config(self) -> dict[str, Any]:
        """Initialize the PIS configuration.

        This method reads the PIS configuration file, replaces the fields defined
        in the unified pipeline config, and returns the resulting configuration.
        """
        pis_raw_conf = read_yaml_config(self.pis_config_local_path)

        # set the work bucket path
        pis_raw_conf["remote_uri"] = f"{self.release_uri}/input"

        # fill in the scratchpad fields
        pis_raw_conf["scratchpad"]["chembl_version"] = self.chembl_version
        pis_raw_conf["scratchpad"]["efo_version"] = self.efo_version
        pis_raw_conf["scratchpad"]["ensembl_version"] = self.ensembl_version

        return pis_raw_conf

    def pis_env_vars(self, step_name: str) -> dict[str, str]:
        """Return the environment variables for a PIS step."""
        return {
            "PIS_STEP": step_name.replace("pis_", ""),
            "PIS_CONFIG_FILE": "/config.yaml",
            "PIS_POOL": str(self.pis_pool),
        }

    def ontoform_args(self, step_name: str) -> list[str]:
        """Return the arguments for the ONTOFORM step."""
        real_step_name = step_name.replace("ontoform_", "")
        return ["--work-dir", self.release_uri, real_step_name]

    # pyhocon returns a ConfigTree, but we can treat it as a dict
    def init_etl_config(self) -> dict[str, Any]:
        """Initialize the ETL configuration.

        This method reads the ETL configuration file, replaces the fields defined
        in the unified pipeline config, and returns the resulting configuration.
        """
        etl_raw_conf = read_hocon_config(
            self.etl_config_local_path,
            sentinels={
                "remote_uri": self.release_uri,
            },
        )

        # ppp - set the write mode to overwrite and remove the data sources
        if self.is_ppp:
            etl_raw_conf["spark-settings"]["write-mode"] = "overwrite"
            etl_raw_conf["evidences"]["data-sources-exclude"] = []

        return etl_raw_conf

    def init_gentropy_settings(self) -> dict[str, Any]:
        """Initialize the gentropy configuration.

        This method reads the gentropy configuration file, replaces the fields defined
        in the unified pipeline config, and returns the resulting configuration.
        """
        return read_yaml_config(
            self.gentropy_config_local_path,
            sentinels={
                "l2g_training": self.l2g_training,
                "release_uri": self.release_uri,
                "gentropy_version": self.gentropy_version,
                "vep_version": self.vep_version,
            },
        )

    def gentropy_step(self, step_name: str) -> dict[str, Any]:
        """Return the config for the gentropy step."""
        real_step_name = step_name.replace("gentropy_", "")
        step = self.gentropy_config["steps"].get(real_step_name)
        if not step:
            raise ValueError(f"Step {real_step_name} not in gentropy config ({self.gentropy_config_local_path}).")
        return step

"""Configuration class for the platform pipeline."""

from pathlib import Path
from typing import Any

from ot_orchestration.utils import read_hocon_config, read_yaml_config


class PlatformConfig:
    """Configuration class for the platform pipeline.

    This class reads the configuration files for the platform dag, PIS and ETL
    applications, performs some operations on them and then exposes the values.

    Some fields in PIS and ETL application configuration files are replaced with
    values from the platform dag configuration, which is the only one the user of
    the orchestrator has to modify to run the pipeline.

    The configuration files are expected to be in the same directory as this file.
    They are:
    - `platform.yaml`: contains the general configuration for the pipeline.
    - `pis.yaml`: contains the configuration for the PIS steps.
    - `etl.conf`: contains the configuration for the ETL steps.
    """

    def __init__(self) -> None:
        self.platform_config_path = Path(__file__).parent / "platform.yaml"
        self.pis_config_local_path = Path(__file__).parent / "pis.yaml"
        self.etl_config_local_path = Path(__file__).parent / "etl.conf"

        # These are hardcoded config values that are not meant to change often.
        # It is safe to extract them from here into the config file in case they
        # become more dynamic. Just add them to self in the initialization so they
        # are available.

        # fmt: off
        # The base image for PIS, the version tag will be appended from the config file.
        pis_image_base = "europe-west1-docker.pkg.dev/open-targets-eu-dev/platform-input-support-test/platform-input-support-test"
        # The base url for the ETL jar, the version will be replaced in from the config file.
        etl_jar_base = "https://github.com/opentargets/platform-etl-backend/releases/download/v{version}/etl-backend-{version}.jar"

        # The disk size for PIS vms, in GB.
        # Note: although not all steps need this much space, it is easier to have a
        # single value for all steps, and the machines are so short-lived that it
        # doesn't matter much with respect to cost.
        self.pis_disk_size = 150

        # The service account and scopes to use (only used by PIS so far).
        # The drive scope is needed to download spreadsheets from Google Drive
        # for the PIS otar step.
        self.service_account = "platform-input-support@open-targets-eu-dev.iam.gserviceaccount.com"
        self.service_account_scopes = ["https://www.googleapis.com/auth/drive"]
        # fmt: on

        # Platform pipeline settings.
        settings = read_yaml_config(self.platform_config_path)
        self.gcs_url = settings["gcs_url"]
        self.chembl_version = settings["chembl_version"]
        self.efo_version = settings["efo_version"]
        self.ensembl_version = settings["ensembl_version"]
        self.is_ppp = settings["is_ppp"]

        # PIS-specific settings.
        self.pis_config = self.init_pis_config()
        pis_version = settings["pis_version"]
        self.pis_image = f"{pis_image_base}:{pis_version}"
        self.pis_step_list = self.pis_config["steps"].keys()
        self.pis_pool = 16  # number of parallel workers inside of each PIS step

        # ETL-specific settings.
        self.etl_config = self.init_etl_config()
        self.etl_config_gcs_uri = f"{self.gcs_url}/output/etl-config.conf"
        etl_version = settings["etl_version"]
        self.etl_jar_origin_url = f"{etl_jar_base.format(version=etl_version)}"
        self.etl_jar_gcs_uri = f"{self.gcs_url}/output/etl-backend-{etl_version}.jar"  # fmt: skip
        self.etl_step_list = settings["etl_steps"]

    def pis_config_gcs_url(self, step_name: str) -> str:
        """Return the google cloud url of the PIS configuration file for a step."""
        return f"{self.gcs_url}/input/pis-config-{step_name}.yaml"

    def pis_config_docker_path(self, step_name: str) -> str:
        """Return the path to the PIS configuration file for a step inside the docker container."""
        return f"/pis-config-{step_name}.yaml"

    def init_pis_config(self) -> dict[str, Any]:
        """Initialize the PIS configuration.

        This method reads the PIS configuration file, replaces the fields defined
        in the orchestrator config, and returns the resulting configuration.
        """
        pis_raw_conf = read_yaml_config(self.pis_config_local_path)

        # set the work bucket path
        pis_raw_conf["gcs_url"] = f"{self.gcs_url}/input"

        # fill in the scratchpad fields
        pis_raw_conf["scratchpad"]["chembl_version"] = self.chembl_version
        pis_raw_conf["scratchpad"]["efo_version"] = self.efo_version
        pis_raw_conf["scratchpad"]["ensembl_version"] = self.ensembl_version

        # ppp - if not ppp, remove 'otar' and 'pppevidence' steps
        if not self.is_ppp:
            pis_raw_conf["steps"].pop("otar", None)
            pis_raw_conf["steps"].pop("ppp_evidence", None)

        return pis_raw_conf

    # pyhocon returns a ConfigTree, but we can treat it as a dict
    def init_etl_config(self) -> Any:
        """Initialize the ETL configuration.

        This method reads the ETL configuration file, replaces the fields defined
        in the orchestrator config, and returns the resulting configuration.
        """
        etl_raw_conf = read_hocon_config(self.etl_config_local_path)

        # set the work bucket paths
        etl_raw_conf["common"]["input"] = f"{self.gcs_url}/input"
        etl_raw_conf["common"]["output-base_path"] = f"{self.gcs_url}/output"

        # fill in the input data versions
        etl_raw_conf["chembl_version"] = self.chembl_version
        etl_raw_conf["ensembl_version"] = self.ensembl_version

        # ppp - set the write mode to overwrite and remove the data sources
        if self.is_ppp:
            etl_raw_conf["spark-settings"]["write-mode"] = "overwrite"
            etl_raw_conf["evidences"]["data-sources-exclude"] = []

        return etl_raw_conf

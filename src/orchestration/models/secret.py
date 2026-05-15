"""Secret management for GCP Secret Manager.

This module provides models for securely referencing and injecting GCP Secret Manager
secrets into Dataproc cluster init actions.

The main building blocks are:

- `Secret` — a reference to a single versioned secret, with validated fields that prevent
  shell injection when secrets are interpolated into init action scripts.
- `Secrets` — a collection of secrets keyed by environment variable name, also validated
  against shell injection.
- `SecretInitAction` — generates a bash init action script that fetches secrets from
  Secret Manager and writes them as JSON files under ``/var/run/secrets/``, then pushes
  that script to GCS as a Dataproc `NodeInitializationAction`.

Examples:
    Construct a single secret reference by field:

    >>> secret = Secret(secret_id="hfhub-key", project_id="my-gcp-project")
    >>> secret.name
    'projects/my-gcp-project/secrets/hfhub-key/versions/latest'

    Construct from a fully-qualified Secret Manager resource name:

    >>> secret = Secret.from_secret_name(
    ...     "projects/my-gcp-project/secrets/hfhub-key/versions/3"
    ... )
    >>> secret.version_id
    '3'

    Build a collection and attach it to a Dataproc init action:

    >>> from airflow.providers.google.cloud.hooks.gcs import GCSHook  # doctest: +SKIP
    >>> secrets = Secrets(
    ...     mapping={
    ...         "HF_TOKEN": Secret(secret_id="hfhub-key", project_id="my-gcp-project"),
    ...         "OPENAI_API_KEY": Secret(
    ...             secret_id="openai-key",
    ...             project_id="my-gcp-project",
    ...             version_id="2",
    ...         ),
    ...     }
    ... )
    >>> init_action = SecretInitAction(
    ...     secrets=secrets,
    ...     init_action_uri="gs://my-bucket/init-actions/inject-secrets.sh",
    ... )
    >>> node_init_action = init_action.push_to_gcs(gcs_hook=gcs_hook)  # doctest: +SKIP

Note:
    All fields on `Secret` and the mapping keys on `Secrets` are validated against
    strict character-set allowlists. This **SHOULD PREVENT** shell injection when values are
    interpolated into the generated bash script.
"""

from __future__ import annotations

import re

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud.dataproc_v1.types import NodeInitializationAction
from pydantic import BaseModel, field_validator

from orchestration.utils.common import GCP_PROJECT_PLATFORM
from orchestration.utils.path import GCSPath


class Secret(BaseModel):
    secret_id: str
    """Secret ID in GCP Secret Manager. This is the name of the secret without the "projects/{project_id}/secrets/" prefix."""
    project_id: str = GCP_PROJECT_PLATFORM
    """Project ID where the secret is stored. Defaults to GCP_PROJECT_PLATFORM."""
    version_id: str = "latest"
    """Version ID of the secret. Defaults to "latest". Can be a specific version number or "latest" to always fetch the most recent version."""

    @field_validator("secret_id")
    @classmethod
    def _validate_secret_id(cls, v: str) -> str:
        """Sanitize the secret_id to ensure it conforms to GCP Secret Manager requirements.

        See: [GCP Secret API](https://docs.cloud.google.com/secret-manager/docs/reference/rest/v1/projects.secrets/create#query-parameters)
        """
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,255}", v):
            raise ValueError(
                f"Invalid secret_id {v!r}. Must be 1-255 characters containing only "
                "letters, digits, hyphens, and underscores."
            )
        return v

    @field_validator("project_id")
    @classmethod
    def _validate_project_id(cls, v: str) -> str:
        """Sanitize the project_id to ensure it conforms to GCP Secret Manager requirements.

        See: [GCP Project ID requirements](https://docs.cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin)
        """
        if not re.fullmatch(r"[a-z0-9-]{6,30}", v):
            raise ValueError(
                f"Invalid project_id {v!r}. Must be 6-30 characters containing only "
                "lowercase letters, digits, and hyphens."
            )
        return v

    @field_validator("version_id")
    @classmethod
    def _validate_version_id(cls, v: str) -> str:
        """Sanitize the version_id to ensure it is either 'latest' or a numeric version string."""
        if not re.fullmatch(r"latest|[0-9]+", v):
            raise ValueError(f"Invalid version_id {v!r}. Must be 'latest' or a numeric version string.")
        return v

    @classmethod
    def from_secret_name(cls, secret_name: str) -> Secret:
        """Create a Secret instance from the full secret name in the format "projects/{project_id}/secrets/{secret_id}/versions/{version_id}"."""
        parts = secret_name.split("/")
        if len(parts) != 6 or parts[0] != "projects" or parts[2] != "secrets" or parts[4] != "versions":
            raise ValueError(
                f"Invalid secret name format: {secret_name}. Expected format: 'projects/{{project_id}}/secrets/{{secret_id}}/versions/{{version_id}}'"
            )
        return cls(
            project_id=parts[1],
            secret_id=parts[3],
            version_id=parts[5],
        )

    @property
    def name(self) -> str:
        """Construct the full secret name in the format required by GCP Secret Manager API."""
        return f"projects/{self.project_id}/secrets/{self.secret_id}/versions/{self.version_id}"


class Secrets(BaseModel):
    """Secrets management for GCP Secret Manager.

    This class represents a collection of secrets that can be injected as environment variables to the batch tasks.
    The mapping between environment variable names and secrets is defined in the `mapping` attribute.

    The `to_env` method can be used to fetch the secret values and convert them to a format suitable for environment variables.

    Examples:
    ---
    >>> secrets = Secrets(mapping={  # doctest: +SKIP
    ...     "MY_ENV_VARIABLE": Secret(
    ...         secret_id="secret_id",
    ...         project_id="my-project",
    ...         version_id="latest",
    ...     ),
    ... })
    >>> secrets = Secrets(mapping={  # doctest: +SKIP
    ...     # By default the `latest` and `GCP_PROJECT_PLATFORM` values will be used
    ...     # for version_id and project_id respectively.
    ...     "MY_ENV_VARIABLE": Secret.from_secret_name("projects/my-project/secrets/secret_id/versions/latest"),
    ... })
    """

    mapping: dict[str, Secret]
    """Mapping between environment variable names and secret ids."""

    @field_validator("mapping")
    @classmethod
    def _validate_env_var_names(cls, v: dict[str, Secret]) -> dict[str, Secret]:
        """Sanitize the environment variable names to ensure they conform to typical environment variable naming conventions."""
        for key in v:
            if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", key):
                raise ValueError(
                    f"Invalid environment variable name {key!r}. Must start with an "
                    "uppercase letter or underscore, followed by uppercase letters, digits, or underscores."
                )
        return v

    def build(self) -> dict[str, str]:
        """Build a dictionary of environment variable names to secret references in the format expected by Dataproc init actions."""
        return {env_var: secret.name for env_var, secret in self.mapping.items()}


class SecretInitAction(BaseModel):
    """Represents a secret that needs to be injected as a file to the batch task using init actions."""

    secrets: Secrets
    """Secrets to be injected as environment variables to the batch tasks."""
    init_action_uri: str
    """The URI of the init action script that will handle the secret injection."""

    @property
    def fetch_secret_cmd(self) -> str:
        """Command template to fetch the secret value using gcloud CLI.

        The command generated by this template creates a file under /var/run/secrets/{secret_id}
        with the JSON blob representing {env_var: secret_value} mapping.

        Returns:
            str: Format string with placeholders for env_var, version_id, secret_id, project_id and secret_file_path.
        """
        # NOTE: DO NOT TOUCH! THIS STRING IS CRAFTED AS A PART OF THE INIT ACTION SCRIPT THAT FETCHES THE SECRETS FROM GCP.
        return 'echo "{{\\"{env_var}\\": \\"$(gcloud secrets versions access {version_id} --secret={secret_id} --project={project_id})\\"}}"  > /var/run/secrets/{secret_id}'

    def _to_script_str(self) -> str:
        """Transform the secrets into a init action script.

        The script will inject secrets using secret manager into the `var/run/secrets/` directory.
        """
        fetch_secret_cmds = [
            self.fetch_secret_cmd.format(
                env_var=env_var,
                version_id=secret.version_id,
                secret_id=secret.secret_id,
                project_id=secret.project_id,
            )
            for env_var, secret in self.secrets.mapping.items()
        ]
        lines = [
            "#!/bin/bash",
            "set -euo pipefail",
            "set +x",
            "mkdir -p /var/run/secrets",
            "# Use gcloud secret-manager to fetch secrets and export them to files under /var/run/secrets/",
            *fetch_secret_cmds,
            "# 112 is the 'hadoop' group",
            "chown root:112 /var/run/secrets/*",
            "chmod 440 /var/run/secrets/*",
        ]
        return "\n".join(lines)

    def push_to_gcs(self, gcs_hook: GCSHook) -> NodeInitializationAction:
        """Push the init action script to GCS.

        Args:
            gcs_hook (GCSHook): The GCS hook to use for uploading the script.

        Note:
            This method is expected to be called from the
            :class:`~airflow.models.baseoperator.BaseOperator` `execute` method

        Returns:
            NodeInitializationAction: The init action to be added to the cluster configuration.
        """
        ob = GCSPath(self.init_action_uri)
        gcs_hook.upload(
            bucket_name=ob.bucket,
            object_name=ob.path,
            data=self._to_script_str(),
        )
        return NodeInitializationAction(
            executable_file=self.init_action_uri,
        )

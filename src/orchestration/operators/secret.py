"""Google Cloud Secret Manager operator implementation."""

from collections.abc import Sequence

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.secret_manager import SecretManagerServiceClient

from orchestration.models.secret import SecretToEnvConfig
from orchestration.operators.secret_to_env import SecretToEnvParserRegistry


class GoogleCloudSecretToEnvOperator(BaseOperator):
    """Custom operator that retrieves a secret from Google Cloud Secret Manager and sets it as an environment variable.

    Args:
        secret_id: The ID of the secret to retrieve.
        project_id: The GCP project ID where the secret is stored.
        env_parser_class_name: The name of the environment parser class associated with the secret.
    """

    template_fields: Sequence[str] = ("secret_id", "project_id", "env_var_name")

    def __init__(
        self,
        *args,
        project_id: str,
        secret_id: str,
        env_parser_class_name: str,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._config = SecretToEnvConfig(
            name=secret_id,
            project_id=project_id,
            env_parser_class_name=env_parser_class_name,
        )

    def execute(self, context) -> None:
        """Execute the Operator."""
        env_parser = SecretToEnvParserRegistry.get_parser(self._config.env_parser_class_name)
        if env_parser is None:
            raise AirflowException(f"No environment parser found for class name: {self._config.env_parser_class_name}")

        client = SecretManagerServiceClient()
        secret_version_name = f"projects/{self._config.project_id}/secrets/{self._config.name}/versions/latest"
        try:
            response = client.access_secret_version(name=secret_version_name)
            secret_value = response.payload.data.decode("UTF-8")
            self.log.info("Retrieved secret value for %s", self._config.name)
            env_vars = env_parser.parse(secret_value)
        except Exception as e:
            raise AirflowException(f"Failed to retrieve or parse secret: {e}")

        ti = context.get("ti")
        if ti is None:
            raise AirflowException("Task instance (ti) not found in context.")
        for var_name, var_value in env_vars.items():
            ti.xcom_push(key=var_name, value=var_value)
            self.log.info("Set environment variable %s from secret %s", var_name, self._config.name)

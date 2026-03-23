"""AWS Secret to Environment Variable Operator."""

from orchestration.operators.secret_to_env import SecretToEnvParser


class AWSSecretToEnvParser(SecretToEnvParser):
    """Parser for AWS secrets to environment variables."""

    REQUIRED_ENV_VARS: tuple[str, str, str] = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION")
    OPTIONAL_ENV_VARS: tuple[str] = ("AWS_ENDPOINT_URL",)

    def _parse(self, secret_value: str) -> dict[str, str]:
        """Parse the AWS secret value and return a dictionary of environment variable names and their corresponding values."""
        import json

        try:
            secret_dict: dict[str, str] = json.loads(secret_value)
            required = {var: secret_dict[var] for var in self.REQUIRED_ENV_VARS if var in secret_dict}
            optional = {var: secret_dict[var] for var in self.OPTIONAL_ENV_VARS if var in secret_dict}
            if len(required) != len(self.REQUIRED_ENV_VARS):
                missing_vars = set(self.REQUIRED_ENV_VARS) - set(required.keys())
                raise ValueError(f"Missing required AWS secret keys: {', '.join(missing_vars)}")
            unknown_vars = set(secret_dict.keys()) - set(self.REQUIRED_ENV_VARS) - set(self.OPTIONAL_ENV_VARS)
            if unknown_vars:
                raise ValueError(f"Unknown keys in AWS secret: {', '.join(unknown_vars)}")
            return {**required, **optional}

        except json.JSONDecodeError:
            raise ValueError("Failed to parse AWS secret.")

"""Base implementation of secret parser."""

from typing import Protocol


class SecretToEnvParser(Protocol):
    """Protocol for parsing secrets from Google Cloud Secret Manager and setting them as environment variables."""

    def _parse(self, secret_value: str) -> dict[str, str]:
        """Parse the secret value and return a dictionary of environment variable names and their corresponding values."""
        ...

    def parse(self, secret_value: str) -> dict[str, str]:
        """Public method to parse the secret value and return a dictionary of environment variable names and their corresponding values."""
        return self._parse(secret_value)


class SecretToEnvParserRegistry:
    """Registry for SecretToEnvParser implementations."""

    _parsers: dict[str, SecretToEnvParser] = {}

    @classmethod
    def get_parser(cls, class_name: str) -> SecretToEnvParser | None:
        """Retrieve a registered SecretToEnvParser implementation by class name."""
        return cls._parsers.get(class_name)

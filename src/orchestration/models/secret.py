"""Model representing a secret in google cloud secret manager."""

from __future__ import annotations

from pydantic import BaseModel


class Secret(BaseModel):
    """Model representing a secret in google cloud secret manager."""

    name: str
    """Name of the secret in google cloud secret manager."""
    project_id: str
    """Google Cloud project ID where the secret is stored."""
    env_parser_class_name: str
    """Name of the environment parser class associated with the secret."""

"""Gentropy config models."""

from typing import Any

from pydantic import BaseModel


class GentropyCommandConfig(BaseModel):
    """Pydantic model for Gentropy steps command configuration."""

    steps: dict[str, dict[str, Any]]
    """Steps available in the gentropy runtime configuration."""

"""Otter task configuration model."""

from typing import Any, Literal

from pydantic import BaseModel


class OtterCommandConfig(BaseModel):
    """Pydantic model for Otter based configurations."""

    work_path: str | None = None
    """Path to the work directory."""
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "SUCCESS"] = "INFO"
    """Log level for the otter steps."""
    scratchpad: dict[str, str] | None = None
    """Scratchpad with replacements for the otter steps."""
    steps: dict[str, list[Any]]
    """List of otter steps"""

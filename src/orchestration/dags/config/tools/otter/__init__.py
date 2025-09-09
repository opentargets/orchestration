from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel


class OtterTaskConfig(BaseModel):
    """Base class for Otter based task configuration."""

    work_path: str = "/mnt/disks/work"
    """Path to the working directory inside the container, if None defaults to /data/work"""
    log_level: Literal["DEBUG", "INFO"] = "INFO"
    """Log level for the task."""
    pool: int | None = 16
    """Number of parallel workers to use, if None defaults to 1"""
    scratchpad: dict[str, str] | None = None
    """Scratchpad to pass values between steps."""
    steps: dict[str, Any]
    """Dictionary of the steps to execute. This can vary depending on the tool implementing otter tasks."""
    release_uri: str | None = None
    """URI to the release info location, if applicable."""

    @property
    def step_names(self) -> list[str]:
        """Get the list of step names."""
        return list(self.steps.keys())

    def get_step_config(self, s: str) -> OtterTaskConfig:
        """Get the configuration for a specific step.

        This will return a new OtterTaskConfig with all top level fields copied from the parent
        object and with the list of tasks limited to the specified step only.

        Args:
            s (str): Step name.

        Returns:
            OtterTaskConfig: Configuration for the step.
        """
        filtered_steps = {s: self.steps.get(s)}
        assert len(filtered_steps.keys()) == 1, f"Found duplicates for step {s}"
        tasks = filtered_steps.get(s)
        assert tasks is not None, f"Empty step {s}."
        assert len(tasks) != 0, f"Empty task list for step {s}"
        return OtterTaskConfig(
            work_path=self.work_path,
            log_level=self.log_level,
            scratchpad=self.scratchpad,
            pool=self.pool,
            steps=filtered_steps,
            release_uri=self.release_uri,
        )

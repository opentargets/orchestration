from typing import Any, Protocol, runtime_checkable

from orchestration.dags.config.unified_pipeline import UnifiedPipelineConfig


@runtime_checkable
class Differ(Protocol):
    def is_diff(
        self,
        *args: Any,
        step_name: str,
        config: UnifiedPipelineConfig,
        **kwargs: Any,
    ) -> bool:
        """Determine whether something has changed.

        Returns True if there are differences, False otherwise. This is used to
        decide if a step should run, so as a general rule, if `is_diff`, then the
        step must run.

        Args:
            *args: Positional arguments.
            step_name (str): The name of the step to compare.
            config (UnifiedPipelineConfig): The unified pipeline configuration.
            **kwargs: Keyword arguments.

        Returns:
            bool: Whether there are differences in the comparison.
        """
        ...

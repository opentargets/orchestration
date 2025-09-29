"""Configuration models."""

import re
from collections import UserString
from typing import Any

from pydantic import GetCoreSchemaHandler
from pydantic_core.core_schema import CoreSchema, ValidationInfo, with_info_plain_validator_function

from orchestration.utils.utils import clean_name


class StepName(UserString):
    """Step name.

    This class validates that the step name has a valid format.
    """

    def __init__(self, name: str) -> None:
        """Check if the name is valid."""
        self.tool, self.step = self._validate_name(name)
        self._name = name
        super().__init__(name)

    @staticmethod
    def _validate_name(name: str):
        if any(c.isupper() for c in name):
            raise ValueError(f"Step name {name} must be lowercase")
        pattern = re.compile(r"^(?P<tool>gentroutils|gentropy)_(?P<step>[a-z0-9-_]+)$")
        m = pattern.match(name)
        if not m:
            raise ValueError(
                f"Step name {name} must start with 'gentroutils_' or 'gentropy_' "
                "and contain only lowercase letters, numbers, hyphens, and underscores"
            )
        return (m.group("tool"), m.group("step"))

    def to_resource_name(self, prefix: str) -> str:
        """Convert the step name to a GCP resource name."""
        return f"{prefix}-{clean_name(self._name)}"

    # @classmethod
    # def validate(cls, value: Any, info: ValidationInfo) -> Any:
    #     if not isinstance(value, str):
    #         raise TypeError("string required")
    #     return cls(value)

    # @classmethod
    # def __get_pydantic_core_schema__(cls, source: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
    #     return with_info_plain_validator_function(
    #         function=cls.validate,
    #     )

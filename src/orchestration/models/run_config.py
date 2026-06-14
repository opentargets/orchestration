"""Pydantic model for unified pipeline run configuration."""

from __future__ import annotations

import dataclasses
import re

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from orchestration.utils.common import GCS_PIPELINE_RUNS_BUCKET


@dataclasses.dataclass(frozen=True)
class _ParsedRunName:
    """Cached parsed result of run_name validation."""
    flavor: str
    yymm: str


# Regex groups: (1)=prefix, (2)=flavor(platform|ppp), (3)=YYMM, (4)=revision
_RUN_NAME_RE = re.compile(r"^([a-z][a-z0-9]*)/(platform|ppp)-(\d{4})-(\d+)$")


class PipelineRunConfig(BaseModel):
    """Validated pipeline run version.

    run_name format: <prefix>/<flavor>-YYMM-N
      prefix — lowercase letter start, then letters or digits (e.g. 'sz', 'pt01')
      flavor — 'platform' for public Platform releases, 'ppp' for Partner Preview
      YYMM   — two-digit year + two-digit month (format-only validation)
      N      — revision integer starting from 1

    All unified pipeline runs write under the pipeline-runs bucket.
    is_ppp is derived from the flavor portion of run_name.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    run_name: str
    _parsed: _ParsedRunName | None = None

    @field_validator("run_name")
    @classmethod
    def _validate_run_name_fmt(cls, v: str) -> str:
        """Validate the format of run_name against the regex."""
        if _RUN_NAME_RE.fullmatch(v) is None:
            raise ValueError(
                f"run_name '{v}' must match '<prefix>/(platform|ppp)-YYMM-N' "
                "(e.g. 'sz/platform-2605-1')"
            )
        return v

    @model_validator(mode="after")
    def _parse_and_validate(self) -> PipelineRunConfig:
        """Parse and cache run_name groups after validation."""
        match = _RUN_NAME_RE.fullmatch(self.run_name) or None
        if match is None:  # pragma: no cover -- should never happen post-validator
            raise ValueError(f"run_name '{self.run_name}' failed internal validation")
        if int(match.group(4)) < 1:
            raise ValueError("run_name revision must be a positive integer starting from 1")
        self._parsed = _ParsedRunName(
            flavor=match.group(2), yymm=match.group(3))
        return self

    @property
    def is_ppp(self) -> bool:
        """True when run_name flavor is 'ppp' (Partner Preview)."""
        if self._parsed is None:
            return False
        return self._parsed.flavor == "ppp"

    @property
    def release_uri(self) -> str:
        """GCS URI for this run's output in the pipeline-runs bucket."""
        if self._parsed is None:
            raise RuntimeError("PipelineRunConfig not yet validated")
        return f"{GCS_PIPELINE_RUNS_BUCKET}/{self.run_name}"

    @property
    def release_name(self) -> str:
        """Canonical release label derived from run_name: '<flavor>-YYMM'.

        Used as ot_release in PTS config and l2g_training_version in Gentropy.
        Personal prefix and revision are stripped.
        """
        if self._parsed is None:
            return ""
        return f"{self._parsed.flavor}-{self._parsed.yymm}"

"""Pydantic model for pipeline run version configuration."""

from __future__ import annotations

import re
from datetime import datetime

from pydantic import BaseModel, field_validator

_RUN_NAME_RE = re.compile(r"^([a-z]+)/(platform|ppp)-(\d{4})-(\d+)$")

_DEV_BUCKET = "gs://open-targets-pipeline-runs"
_RELEASE_BUCKET = "gs://open-targets-pre-data-releases"


class PipelineRunConfig(BaseModel):
    """Validated pipeline run version.

    run_name format: <prefix>/<flavor>-YYMM-N
      prefix  — lowercase letters only, e.g. 'sz'
      flavor  — 'platform' or 'ppp'
      YYMM    — two-digit year + two-digit month, e.g. '2605' for May 2026
      N       — revision integer, e.g. '1'

    Examples: 'sz/platform-2605-1', 'abc/ppp-2606-2'
    """

    run_name: str
    is_dev: bool = True

    @field_validator("run_name")
    @classmethod
    def _validate_run_name(cls, v: str) -> str:
        match = _RUN_NAME_RE.fullmatch(v)
        if not match:
            raise ValueError(
                f"run_name '{v}' must match '<prefix>/(platform|ppp)-YYMM-N' "
                "(e.g. 'sz/platform-2605-1')"
            )
        yymm = int(match.group(3))
        current_yymm = int(datetime.now().strftime("%y%m"))
        if yymm < current_yymm:
            raise ValueError(
                f"run_name date '{match.group(3)}' is in the past "
                f"(current: {current_yymm:04d}). "
                "Update run_name to the current or a future YYMM to avoid "
                "overwriting an existing release."
            )
        return v

    @property
    def release_uri(self) -> str:
        """GCS URI for this run's output.

        Returns the dev bucket path when is_dev=True, otherwise the production
        release bucket path using only flavor-YYMM (no prefix or revision).
        """
        if self.is_dev:
            return f"{_DEV_BUCKET}/{self.run_name}"
        return f"{_RELEASE_BUCKET}/{self.release_name}"

    @property
    def release_name(self) -> str:
        """Canonical release label derived from run_name: '<flavor>-YYMM'.

        Used as ot_release in PTS config and l2g_training_version in Gentropy.
        Personal prefix and revision are stripped.
        """
        match = _RUN_NAME_RE.fullmatch(self.run_name)
        assert match  # already validated
        flavor = match.group(2)
        yymm = match.group(3)
        return f"{flavor}-{yymm}"

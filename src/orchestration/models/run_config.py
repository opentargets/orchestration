"""Pydantic model for pipeline run version configuration."""

from __future__ import annotations

import re
from datetime import datetime

from pydantic import BaseModel, ConfigDict, field_validator

from orchestration.utils.common import GCS_PIPELINE_RUNS_BUCKET, GCS_PRE_DATA_RELEASES_BUCKET

_RUN_NAME_RE = re.compile(r"^([a-z]+)/(platform|ppp)-(\d{4})-(\d+)$")


class PipelineRunConfig(BaseModel):
    """Validated pipeline run version.

    run_name format: <prefix>/<flavor>-YYMM-N
      prefix  — lowercase letters only, e.g. 'sz'
      flavor  — 'platform' or 'ppp'
      YYMM    — two-digit year + two-digit month, e.g. '2605' for May 2026
      N       — revision integer, e.g. '1'

    Examples: 'sz/platform-2605-1', 'abc/ppp-2606-2'
    """

    model_config = ConfigDict(frozen=True)

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
        mm = int(match.group(3)[2:])
        if not (1 <= mm <= 12):
            raise ValueError(
                f"run_name month '{match.group(3)[2:]}' is not a valid calendar month (01-12)"
            )
        return v

    @property
    def release_uri(self) -> str:
        """GCS URI for this run's output.

        Returns the dev bucket path when is_dev=True, otherwise the production
        release bucket path using only flavor-YYMM (no prefix or revision).
        """
        if self.is_dev:
            return f"{GCS_PIPELINE_RUNS_BUCKET}/{self.run_name}"
        return f"{GCS_PRE_DATA_RELEASES_BUCKET}/{self.release_name}"

    @property
    def release_name(self) -> str:
        """Canonical release label derived from run_name: '<flavor>-YYMM'.

        Used as ot_release in PTS config and l2g_training_version in Gentropy.
        Personal prefix and revision are stripped.
        """
        match = _RUN_NAME_RE.fullmatch(self.run_name)
        if match is None:
            raise RuntimeError(f"run_name '{self.run_name}' failed internal validation")
        flavor = match.group(2)
        yymm = match.group(3)
        return f"{flavor}-{yymm}"

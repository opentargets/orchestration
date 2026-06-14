"""Unified pipeline run configuration."""

from __future__ import annotations

import re

from orchestration.utils.common import GCS_PIPELINE_RUNS_BUCKET

_RUN_NAME_RE = re.compile(r"^([a-z][a-z0-9]*)/(platform|ppp)-(\d{4})-(\d+)$")


class PipelineRunConfig:
    """Validated unified pipeline run configuration."""

    __slots__ = ("is_ppp", "release_name", "release_uri", "run_name")

    def __init__(self, run_name: str) -> None:
        match = _RUN_NAME_RE.fullmatch(run_name)
        if match is None:
            raise ValueError(
                f"run_name '{run_name}' must match '<prefix>/(platform|ppp)-YYMM-N' "
                "(e.g. 'sz/platform-2605-1')"
            )

        revision = int(match.group(4))
        if revision < 1:
            raise ValueError("run_name revision must be a positive integer starting from 1")

        flavor = match.group(2)
        yymm = match.group(3)

        self.run_name = run_name
        self.is_ppp = flavor == "ppp"
        self.release_name = f"{flavor}-{yymm}"
        self.release_uri = f"{GCS_PIPELINE_RUNS_BUCKET}/{run_name}"

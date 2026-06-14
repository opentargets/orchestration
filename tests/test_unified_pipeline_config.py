from pathlib import Path

import pytest
from pydantic import ValidationError

from orchestration.dags.config.app_config import AppConfig
from orchestration.dags.config.unified_pipeline import UnifiedPipelineConfig


@pytest.mark.parametrize("is_dev_value", [True, None])
def test_unified_pipeline_rejects_legacy_is_dev(
    monkeypatch: pytest.MonkeyPatch, is_dev_value: bool | None
) -> None:
    class StageConfigConsumedError(ValueError):
        """Sentinel raised if stage config loading happens before legacy rejection."""

    class FakeUnifiedPipelineConfig:
        def __init__(self, is_dev_value: bool | None) -> None:
            self.config = {
                "run_name": "sz/platform-2605-1",
                "is_dev": is_dev_value,
                "steps": {},
            }

        def get(self, key_or_path: str, default=None):
            return self.config.get(key_or_path, default)

    def fake_from_file(file_path, client=None, template_context=None):
        path = Path(file_path)
        if path.name == "unified_pipeline.yaml":
            return FakeUnifiedPipelineConfig(is_dev_value=is_dev_value)
        raise StageConfigConsumedError(f"stage config loaded before legacy is_dev was rejected: {path.name}")

    monkeypatch.setattr(AppConfig, "from_file", fake_from_file)

    with pytest.raises(
        (ValueError, ValidationError),
        match=r"is_dev.*(rejected|unsupported|no longer supported|forbidden)|legacy is_dev.*rejected",
    ) as excinfo:
        UnifiedPipelineConfig()

    assert not isinstance(excinfo.value, StageConfigConsumedError)

"""Test staging configurations."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from orchestration.dags.config.staging import StagingPipelineConfig, StagingPipelineConfigModel


class TestStagingConfigs:
    @pytest.mark.parametrize("config", [pytest.param("gwas_catalog_update.yaml")])
    def test_staging_pipeline_config(self, config: str) -> None:
        """Test with existing staging dag configuration."""
        config_path = Path("src/orchestration/dags/config/staging") / config
        assert config_path.exists(), f"Config path {config_path} does not exist."
        assert config_path.is_file(), f"Config path {config_path} is not a file."
        staging_config = StagingPipelineConfig(path=config_path)
        assert staging_config.raw is not None, "Raw config is None"
        assert staging_config.raw.validated is not None, "Validated config is None"
        assert isinstance(staging_config.raw.validated, StagingPipelineConfigModel), (
            "Validated config is not of type StagingPipelineConfigModel"
        )


class TestStagingPipelineConfigModel:
    def test_environment_specs_validation_empty_list(self) -> None:
        """Test that empty environment_specs list raises validation error."""
        invalid_config = {"environment_specs": [], "env": "prod", "steps": {}}

        with pytest.raises(ValidationError) as exc_info:
            StagingPipelineConfigModel.model_validate(invalid_config)

        assert "environment_specs cannot be empty" in str(exc_info.value)

    def test_environment_specs_validation_valid_list(self) -> None:
        """Test that non-empty environment_specs list passes validation."""
        valid_config = {"environment_specs": [{"name": "prod", "vars": {"key": "value"}}], "env": "prod", "steps": {}}

        # This should not raise any validation error
        config_model = StagingPipelineConfigModel.model_validate(valid_config)
        assert len(config_model.environment_specs) == 1
        assert config_model.environment_specs[0].name == "prod"

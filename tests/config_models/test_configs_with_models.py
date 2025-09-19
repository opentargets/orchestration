"""Test gentropy config with pydantic model."""

from pathlib import Path

import pytest
from pydantic import BaseModel

from orchestration.dags.config.app_config import AppConfig
from orchestration.dags.config.staging_config import (
    DataprocConfig,
    GentropyCommandConfig,
    GentropyPipelineConfig,
    OtterCommandConfig,
    StagingPipelineConfig,
)


class TestGentropyConfigModel:
    """Test gentropy config with pydantic model."""

    @pytest.mark.parametrize(
        ("config_file_basename", "model_class"),
        [
            pytest.param("gentropy.yaml", GentropyCommandConfig, id="gentropy.yaml"),
            pytest.param("clusters.yaml", DataprocConfig, id="clusters.yaml"),
            pytest.param("gentroutils.yaml", OtterCommandConfig, id="gentroutils.yaml"),
            pytest.param("pis.yaml", OtterCommandConfig, id="pis.yaml"),
            pytest.param("pts.yaml", OtterCommandConfig, id="pts.yaml"),
        ],
    )
    def test_common_configs(
        self,
        config_dir: Path,
        config_file_basename: str,
        model_class: type[BaseModel],
    ) -> None:
        """Test common config models."""
        config_path = config_dir / config_file_basename
        assert config_path.exists(), f"Config path {config_path} does not exist"
        assert config_path.is_file(), f"Config path {config_path} is not a file"

        # Assert no errors are raised during validation
        c = AppConfig.from_file(config_path)
        v = c.validate(model_class)
        assert isinstance(v, model_class)

    @pytest.mark.parametrize(
        ("config_file_basename", "model_class"),
        [
            pytest.param("gwas_catalog_top_hits.yaml", StagingPipelineConfig, id="gwas_catalog_top_hits.yaml"),
        ],
    )
    def test_gentropy_pipeline_configs(
        self,
        config_dir: Path,
        config_file_basename: str,
        model_class: type[BaseModel],
    ) -> None:
        """Test staging pipeline config model."""
        config_path = config_dir / config_file_basename
        assert config_path.exists(), f"Config path {config_path} does not exist"
        assert config_path.is_file(), f"Config path {config_path} is not a file"

        # Assert no errors are raised during validation
        c = AppConfig.from_file(config_path)
        v = c.validate(model_class)
        assert isinstance(v, model_class)

        # Assert GentropyPipelineConfig can be instantiated
        gpc = GentropyPipelineConfig.read_config(str(config_path))
        assert isinstance(gpc, GentropyPipelineConfig)
        assert hasattr(gpc, "dataproc_config")
        assert isinstance(gpc.dataproc_config, DataprocConfig)
        assert hasattr(gpc, "gentropy_runtime_config")
        assert isinstance(gpc.gentropy_runtime_config, GentropyCommandConfig)

"""Test common functions."""

from typing import Any

import pytest

from orchestration.utils import convert_params_to_hydra_positional_arg


@pytest.mark.parametrize(
    ("params", "output", "error"),
    [
        pytest.param(
            {"step.param1": "value1", "step.param2": "value2"},
            ["step.param1=value1", "step.param2=value2"],
            False,
            id="Step with two parameters.",
        ),
        pytest.param(
            [],
            None,
            ValueError,
            id="Step without parameters.",
        ),
        pytest.param(
            {"+step.param1": "value1"},
            ["+step.param1=value1"],
            False,
            id="Step with + parameter.",
        ),
    ],
)
def test_convert_params_to_hydra_positional_arg(params: dict[str, Any] | None, output: list[str], error: Any) -> None:
    """Test conversion between step configuration and hydra positional arguments."""
    if error:
        with pytest.raises(error):
            convert_params_to_hydra_positional_arg(params)
    else:
        assert convert_params_to_hydra_positional_arg(params) == output


# ─── CustomClusterConfig asset sync fields ──────────────────────────────────


def test_custom_cluster_config_accepts_init_actions_assets() -> None:
    """CustomClusterConfig should accept init_actions_assets without error."""
    from orchestration.operators.dataproc import CustomClusterConfig

    config = CustomClusterConfig(
        cluster_name="test",
        init_actions_assets=["install_gentropy_on_cluster.sh"],
    )
    assert config.init_actions_assets == ["install_gentropy_on_cluster.sh"]


def test_custom_cluster_config_accepts_init_actions_asset_location() -> None:
    """CustomClusterConfig should accept a per-cluster asset location override."""
    from orchestration.operators.dataproc import CustomClusterConfig

    config = CustomClusterConfig(
        cluster_name="test",
        init_actions_assets=["foo.sh"],
        init_actions_asset_location="gs://custom-bucket/scripts/",
    )
    assert config.init_actions_asset_location == "gs://custom-bucket/scripts/"


def test_create_cluster_excludes_asset_fields() -> None:
    """create_cluster should strip asset-specific fields from the Dataproc API payload."""
    from orchestration.operators.dataproc import CustomClusterConfig

    config = CustomClusterConfig(
        cluster_name="test",
        init_actions_assets=["foo.sh"],
        init_actions_asset_location="gs://custom-bucket/scripts/",
    )
    cluster_cfg = config.create_cluster()
    assert "init_actions_assets" not in cluster_cfg
    assert "init_actions_asset_location" not in cluster_cfg


# ─── CreateClusterOperator asset helpers (unit) ──────────────────────────────


def test_resolve_asset_location_uses_override() -> None:
    """_resolve_asset_location returns the per-cluster override when set."""
    from unittest.mock import MagicMock

    from orchestration.operators.dataproc import CreateClusterOperator, CustomClusterConfig

    cluster_config = CustomClusterConfig(
        init_actions_asset_location="gs://override-bucket/path/",
    )

    # _resolve_asset_location checks self._cluster_config and self.cluster_type
    mock_op = MagicMock()
    mock_op._cluster_config = cluster_config
    mock_op.cluster_type = "gentropy-abc123"

    location = CreateClusterOperator._resolve_asset_location(mock_op)  # type: ignore[arg-type]
    assert location == "gs://override-bucket/path/"


def test_resolve_asset_location_defaults_to_cluster_type() -> None:
    """_resolve_asset_location derives default bucket from cluster type name."""
    from unittest.mock import MagicMock

    from orchestration.operators.dataproc import (
        DEFAULT_ASSET_SYNC_BASE,
        CreateClusterOperator,
        CustomClusterConfig,
    )

    cluster_config = CustomClusterConfig(cluster_name="test")
    mock_op = MagicMock()
    mock_op._cluster_config = cluster_config
    mock_op.cluster_type = "pts-abc123"

    location = CreateClusterOperator._resolve_asset_location(mock_op)  # type: ignore[arg-type]
    assert location == f"{DEFAULT_ASSET_SYNC_BASE}pts/"


def test_prepare_asset_init_actions_returns_none_when_no_assets() -> None:
    """When no assets are configured, _prepare returns None."""
    from unittest.mock import MagicMock

    from orchestration.operators.dataproc import CreateClusterOperator, CustomClusterConfig

    cluster_config = CustomClusterConfig(cluster_name="test")
    mock_op = MagicMock()
    mock_op._cluster_config = cluster_config

    result = CreateClusterOperator._prepare_asset_init_actions(mock_op)  # type: ignore[arg-type]
    assert result is None


def test_prepare_asset_init_actions_raises_on_missing_file(monkeypatch) -> None:
    """_prepare raises AirflowException when a listed asset does not exist."""
    from unittest.mock import MagicMock, patch

    import pytest
    from airflow.exceptions import AirflowException

    from orchestration.operators.dataproc import CreateClusterOperator, CustomClusterConfig

    cluster_config = CustomClusterConfig(
        cluster_name="test",
        init_actions_assets=["nonexistent_asset.sh"],
    )
    mock_op = MagicMock()
    mock_op._cluster_config = cluster_config

    # Mock GCSHook to avoid Airflow connection lookup during the call
    with patch("orchestration.operators.dataproc.GCSHook") as mock_hook_cls:
        with pytest.raises(AirflowException, match="Asset file not found"):
            CreateClusterOperator._prepare_asset_init_actions(mock_op)  # type: ignore[arg-type]

        mock_hook_cls.assert_called_once()


def test_sync_asset_to_gcs_calls_upload(tmp_path) -> None:
    """_sync_asset_to_gcs reads the local file and uploads it to GCS."""
    from unittest.mock import MagicMock

    from orchestration.operators.dataproc import CreateClusterOperator, CustomClusterConfig

    # Create a real temp asset file
    asset_file = tmp_path / "test_sync.sh"
    asset_file.write_text("#!/bin/bash\necho hello")

    hook_mock = MagicMock()
    mock_op = MagicMock(spec=CreateClusterOperator)
    mock_op._cluster_config = CustomClusterConfig(cluster_name="test")

    node_action = CreateClusterOperator._sync_asset_to_gcs(
        mock_op, asset_file, "gs://my-bucket/path/test_sync.sh", hook_mock
    )  # type: ignore[arg-type]

    hook_mock.upload.assert_called_once_with(
        bucket_name="my-bucket",
        object_name="path/test_sync.sh",
        data="#!/bin/bash\necho hello",
    )
    assert node_action.executable_file == "gs://my-bucket/path/test_sync.sh"


def test_prepare_asset_init_actions_integration(monkeypatch, tmp_path) -> None:
    """End-to-end happy path: resolve location, read asset, upload via GCSHook, return actions."""
    from unittest.mock import MagicMock

    from google.cloud.dataproc_v1.types import NodeInitializationAction

    from orchestration.operators.dataproc import (
        CreateClusterOperator,
        CustomClusterConfig,
    )

    # Create a real asset file that mimics the installed gentropy script
    asset_file = tmp_path / "install_gentropy_on_cluster.sh"
    asset_file.write_text("#!/bin/bash\necho installing gentropy deps")

    cluster_config = CustomClusterConfig(
        init_actions_assets=["install_gentropy_on_cluster.sh"],
        init_actions_asset_location="gs://my-bucket/init-actions/",
    )
    mock_op = MagicMock()
    mock_op._cluster_config = cluster_config
    mock_op.cluster_type = "default-test"

    # Verify bucket resolution produces the right base
    location = CreateClusterOperator._resolve_asset_location(mock_op)  # type: ignore[arg-type]
    assert location == "gs://my-bucket/init-actions/"

    # Verify _sync_asset_to_gcs calls hook.upload correctly and returns correct action
    base_uri = location.rstrip("/")  # avoid double-slash
    hook_mock = MagicMock()
    node_action = CreateClusterOperator._sync_asset_to_gcs(  # type: ignore[arg-type]
        mock_op, asset_file, f"{base_uri}/install_gentropy_on_cluster.sh", hook_mock
    )

    hook_mock.upload.assert_called_once_with(
        bucket_name="my-bucket",
        object_name="init-actions/install_gentropy_on_cluster.sh",
        data="#!/bin/bash\necho installing gentropy deps",
    )
    assert isinstance(node_action, NodeInitializationAction)
    assert node_action.executable_file == "gs://my-bucket/init-actions/install_gentropy_on_cluster.sh"


def test_prepare_asset_defaults_bucket_from_cluster_type(monkeypatch) -> None:
    """Verify default bucket derivation uses the cluster name prefix."""
    from pathlib import Path
    from unittest.mock import MagicMock, patch

    from airflow.exceptions import AirflowException

    from orchestration.operators.dataproc import (
        DEFAULT_ASSET_SYNC_BASE,
        CreateClusterOperator,
        CustomClusterConfig,
    )

    cluster_config = CustomClusterConfig(
        init_actions_assets=["foo.sh"],
    )
    mock_op = MagicMock()
    mock_op._cluster_config = cluster_config
    mock_op.cluster_type = "default-test"
    mock_op.gcp_conn_id = "google_cloud_default"
    mock_op.impersonation_chain = None

    hook_mock = MagicMock()
    with patch("orchestration.operators.dataproc.ASSET_PATH", Path("/nonexistent")), \
         patch("orchestration.operators.dataproc.GCSHook", return_value=hook_mock) as mock_hook_cls:
        # We expect a file-not-found error, but the bucket resolution and GCSHook creation must have happened first
        with pytest.raises(AirflowException, match="Asset file not found"):
            CreateClusterOperator._prepare_asset_init_actions(mock_op)  # type: ignore[arg-type]

    mock_hook_cls.assert_called_once_with(
        gcp_conn_id="google_cloud_default", impersonation_chain=None
    )

    # Verify the resolved location was correct by checking _resolve_asset_location
    location = CreateClusterOperator._resolve_asset_location(mock_op)  # type: ignore[arg-type]
    assert location == f"{DEFAULT_ASSET_SYNC_BASE}default/"


def test_create_cluster_excludes_asset_fields_from_model_dump() -> None:
    """init_actions_assets and init_actions_asset_location are excluded from model dump."""
    from orchestration.operators.dataproc import CustomClusterConfig

    config = CustomClusterConfig(
        cluster_name="test",
        num_workers=2,
        init_actions_assets=["foo.sh"],
        init_actions_asset_location="gs://custom/path/",
    )
    dumped = config.model_dump(
        exclude={
            "secondary_worker_disk_type",
            "secondary_worker_disk_size",
            "secondary_worker_machine_type",
            "secret_map",
            "secret_init_action_uri",
        }
    )
    assert "init_actions_assets" in dumped
    # But create_cluster() should exclude them properly
    cluster_cfg = config.create_cluster()
    assert "init_actions_assets" not in cluster_cfg

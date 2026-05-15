"""Tests for the secret management module."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from orchestration.models.secret import Secret, SecretInitAction, Secrets


class TestSecretConstruction:
    def test_defaults(self) -> None:
        s = Secret(secret_id="my-secret")
        assert s.version_id == "latest"

    def test_name_property(self) -> None:
        s = Secret(secret_id="my-secret", project_id="my-project", version_id="3")
        assert s.name == "projects/my-project/secrets/my-secret/versions/3"

    def test_name_with_latest(self) -> None:
        s = Secret(secret_id="my-secret", project_id="my-project")
        assert s.name.endswith("/versions/latest")


class TestSecretFromSecretName:
    def test_valid_full_name(self) -> None:
        s = Secret.from_secret_name("projects/my-proj/secrets/hfhub-key/versions/2")
        assert s.project_id == "my-proj"
        assert s.secret_id == "hfhub-key"
        assert s.version_id == "2"

    def test_valid_full_name_latest(self) -> None:
        s = Secret.from_secret_name("projects/my-proj/secrets/hfhub-key/versions/latest")
        assert s.version_id == "latest"

    @pytest.mark.parametrize(
        "name",
        [
            "projects/my-proj/secrets/hfhub-key",  # too short
            "my-proj/secrets/hfhub-key/versions/1",  # missing 'projects' prefix
            "projects/my-proj/hfhub-key/versions/1",  # missing 'secrets' segment
            "projects/my-proj/secrets/hfhub-key/1",  # missing 'versions' segment
            "",
        ],
        ids=["too-short", "no-projects", "no-secrets", "no-versions", "empty"],
    )
    def test_invalid_format_raises(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid secret name format"):
            Secret.from_secret_name(name)


class TestSecretIdValidation:
    @pytest.mark.parametrize(
        "secret_id",
        [
            pytest.param("my-secret", id="hyphen"),
            pytest.param("MY_SECRET", id="uppercase-underscore"),
            pytest.param("abc123", id="alphanumeric"),
            pytest.param("a" * 255, id="max-length"),
            pytest.param("a-B_1", id="mixed"),
        ],
    )
    def test_valid_secret_ids(self, secret_id: str) -> None:
        assert Secret(secret_id=secret_id, project_id="my-proj").secret_id == secret_id

    @pytest.mark.parametrize(
        "secret_id",
        [
            pytest.param("", id="empty"),
            pytest.param("a" * 256, id="too-long"),
            pytest.param("has space", id="space"),
            pytest.param("has$dollar", id="dollar"),
            pytest.param("has$(injection)", id="subshell"),
            pytest.param("semi;colon", id="semicolon"),
            pytest.param("back`tick`", id="backtick"),
        ],
    )
    def test_invalid_secret_ids_raise(self, secret_id: str) -> None:
        with pytest.raises(ValidationError):
            Secret(secret_id=secret_id, project_id="my-proj")


class TestProjectIdValidation:
    @pytest.mark.parametrize(
        "project_id",
        ["my-proj", "open-targets-eu-dev", "abc123", "abcdef"],
        ids=["hyphenated", "real-project", "alphanumeric", "min-length"],
    )
    def test_valid_project_ids(self, project_id: str) -> None:
        assert Secret(secret_id="my-secret", project_id=project_id).project_id == project_id

    @pytest.mark.parametrize(
        "project_id",
        [
            pytest.param("ab", id="too-short"),
            pytest.param("a" * 31, id="too-long"),
            pytest.param("UPPERCASE", id="uppercase"),
            pytest.param("has space", id="space"),
            pytest.param("has$dollar", id="dollar"),
            pytest.param("has$(injection)", id="subshell"),
        ],
    )
    def test_invalid_project_ids_raise(self, project_id: str) -> None:
        with pytest.raises(ValidationError):
            Secret(secret_id="my-secret", project_id=project_id)


class TestVersionIdValidation:
    @pytest.mark.parametrize(
        "version_id",
        [
            pytest.param("latest", id="latest"),
            pytest.param("1", id="one"),
            pytest.param("42", id="two-digit"),
            pytest.param("999", id="three-digit"),
        ],
    )
    def test_valid_version_ids(self, version_id: str) -> None:
        assert Secret(secret_id="my-secret", project_id="my-proj", version_id=version_id).version_id == version_id

    @pytest.mark.parametrize(
        "version_id",
        [
            pytest.param("LATEST", id="uppercase-latest"),
            pytest.param("v1", id="v-prefix"),
            pytest.param("1.0", id="semver"),
            pytest.param("", id="empty"),
            pytest.param("latest1", id="latest-suffix"),
            pytest.param("$(cmd)", id="subshell"),
        ],
    )
    def test_invalid_version_ids_raise(self, version_id: str) -> None:
        with pytest.raises(ValidationError):
            Secret(secret_id="my-secret", project_id="my-proj", version_id=version_id)


class TestSecretsEnvVarValidation:
    def _secret(self) -> Secret:
        return Secret(secret_id="my-secret", project_id="my-proj")

    @pytest.mark.parametrize(
        "env_var",
        [
            pytest.param("MY_VAR", id="normal"),
            pytest.param("_PRIVATE", id="underscore-prefix"),
            pytest.param("A", id="single-char"),
            pytest.param("VAR123", id="alphanumeric"),
            pytest.param("UPPER_CASE_123", id="mixed"),
        ],
    )
    def test_valid_env_var_names(self, env_var: str) -> None:
        secrets = Secrets(mapping={env_var: self._secret()})
        assert env_var in secrets.mapping

    @pytest.mark.parametrize(
        "env_var",
        [
            pytest.param("lowercase", id="lowercase"),
            pytest.param("123start", id="digit-start"),
            pytest.param("has space", id="space"),
            pytest.param("HAS$DOLLAR", id="dollar"),
            pytest.param("HAS$(injection)", id="subshell"),
            pytest.param("", id="empty"),
        ],
    )
    def test_invalid_env_var_names_raise(self, env_var: str) -> None:
        with pytest.raises(ValidationError):
            Secrets(mapping={env_var: self._secret()})

    def test_multiple_valid_entries(self) -> None:
        secrets = Secrets(
            mapping={
                "HF_TOKEN": self._secret(),
                "OPENAI_KEY": Secret(secret_id="openai-key", project_id="my-proj", version_id="2"),
            }
        )
        assert len(secrets.mapping) == 2


@pytest.fixture
def init_action() -> SecretInitAction:
    return SecretInitAction(
        secrets=Secrets(
            mapping={
                "HF_TOKEN": Secret(secret_id="hfhub-key", project_id="my-proj", version_id="latest"),
                "OPENAI_KEY": Secret(secret_id="openai-key", project_id="my-proj", version_id="3"),
            }
        ),
        init_action_uri="gs://my-bucket/init-actions/inject-secrets.sh",
    )


class TestSecretInitActionScript:
    def test_script_has_shebang(self, init_action: SecretInitAction) -> None:
        assert init_action._to_script_str().startswith("#!/bin/bash")

    def test_script_has_strict_mode(self, init_action: SecretInitAction) -> None:
        script = init_action._to_script_str()
        assert "set -euo pipefail" in script

    def test_script_suppresses_trace(self, init_action: SecretInitAction) -> None:
        script = init_action._to_script_str()
        assert "set +x" in script

    def test_script_creates_secrets_dir(self, init_action: SecretInitAction) -> None:
        assert "mkdir -p /var/run/secrets" in init_action._to_script_str()

    def test_script_sets_permissions(self, init_action: SecretInitAction) -> None:
        script = init_action._to_script_str()
        assert "chown root:112 /var/run/secrets/*" in script
        assert "chmod 440 /var/run/secrets/*" in script

    def test_script_contains_all_secret_ids(self, init_action: SecretInitAction) -> None:
        script = init_action._to_script_str()
        assert "hfhub-key" in script
        assert "openai-key" in script

    def test_script_contains_all_env_var_names(self, init_action: SecretInitAction) -> None:
        script = init_action._to_script_str()
        assert "HF_TOKEN" in script
        assert "OPENAI_KEY" in script

    def test_script_uses_correct_version(self, init_action: SecretInitAction) -> None:
        script = init_action._to_script_str()
        assert "--secret=openai-key" in script
        # version 3 appears for openai-key
        assert "access 3" in script

    def test_no_indentation_in_output(self, init_action: SecretInitAction) -> None:
        for line in init_action._to_script_str().splitlines():
            assert not line.startswith(" "), f"Line has leading whitespace: {line!r}"


class TestSecretInitActionPushToGcs:
    def test_push_calls_upload_with_script(self, init_action: SecretInitAction) -> None:
        mock_hook = MagicMock()
        init_action.push_to_gcs(gcs_hook=mock_hook)

        mock_hook.upload.assert_called_once()
        call_kwargs = mock_hook.upload.call_args.kwargs
        assert call_kwargs["bucket_name"] == "my-bucket"
        assert call_kwargs["object_name"] == "init-actions/inject-secrets.sh"
        assert "#!/bin/bash" in call_kwargs["data"]

    def test_push_returns_node_init_action(self, init_action: SecretInitAction) -> None:
        from google.cloud.dataproc_v1.types import NodeInitializationAction

        mock_hook = MagicMock()
        result = init_action.push_to_gcs(gcs_hook=mock_hook)
        assert isinstance(result, NodeInitializationAction)
        assert result.executable_file == "gs://my-bucket/init-actions/inject-secrets.sh"

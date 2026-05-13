"""Tests for RunnableSpec script parsing methods against real asset scripts."""

from importlib.resources import files

import pytest

from orchestration.models.batch.runnable import RunnableSpec

ANNOTATE_TRANSCRIPTS = files("orchestration.assets").joinpath("annotate_transcripts.sh").read_text()
ANNOTATE_VARIANTS = files("orchestration.assets").joinpath("annotate_variants.sh").read_text()


@pytest.mark.parametrize(
    ("script", "expected_line_count"),
    [
        pytest.param(ANNOTATE_TRANSCRIPTS, 23, id="annotate_transcripts"),
        pytest.param(ANNOTATE_VARIANTS, 35, id="annotate_variants"),
    ],
)
def test_remove_comments_line_count(script: str, expected_line_count: int) -> None:
    assert len(RunnableSpec._remove_comments(script)) == expected_line_count


@pytest.mark.parametrize(
    ("script", "expected_count"),
    [
        pytest.param(ANNOTATE_TRANSCRIPTS, 5, id="annotate_transcripts"),
        pytest.param(ANNOTATE_VARIANTS, 5, id="annotate_variants"),
    ],
)
def test_join_continuation_lines_count(script: str, expected_count: int) -> None:
    lines = RunnableSpec._remove_comments(script)
    assert len(RunnableSpec._join_continuation_lines(lines)) == expected_count


@pytest.mark.parametrize(
    ("script", "expected_count"),
    [
        pytest.param(ANNOTATE_TRANSCRIPTS, 5, id="annotate_transcripts"),
        pytest.param(ANNOTATE_VARIANTS, 5, id="annotate_variants"),
    ],
)
def test_split_commands_count(script: str, expected_count: int) -> None:
    lines = RunnableSpec._remove_comments(script)
    joined = RunnableSpec._join_continuation_lines(lines)
    assert len(RunnableSpec._split_commands(joined)) == expected_count


@pytest.mark.parametrize(
    ("script", "expected_count"),
    [
        pytest.param(ANNOTATE_TRANSCRIPTS, 5, id="annotate_transcripts"),
        pytest.param(ANNOTATE_VARIANTS, 5, id="annotate_variants"),
    ],
)
def test_parse_script_file_command_count(script: str, expected_count: int) -> None:
    result = RunnableSpec._parse_script_file(script)
    assert len(result.split(" && ")) == expected_count


def test_parse_script_file_preserves_or_guards() -> None:
    """Parser must not split || guard clauses into separate && commands."""
    script = '[[ -n "${X}" ]] || { echo "X is not set"; exit 1; }'
    result = RunnableSpec._parse_script_file(script)
    assert "|| {" in result, "|| guard clauses must be preserved, not split into &&"

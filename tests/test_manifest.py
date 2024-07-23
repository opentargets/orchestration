from ot_orchestration.utils.manifest import extract_study_id_from_path
import pytest


@pytest.mark.parametrize(
    ["path", "identifier"],
    [
        pytest.param(
            "gs://gentropy-tmp/raw_summary_statistics/GCST004001-GCST005000/GCST004600/",
            "GCST004600",
            id="google cloud path containing id",
        ),
        pytest.param(
            "GCST005000/GCST004617/harmonised/27863252-GCST004617-EFO_0007996.h.tsv.gz",
            "GCST004617",
            id="path with filename containing id",
        ),
    ],
)
def test_extract_study_id_from_path(path: str, identifier: str) -> None:
    """Test extracting study id from paths."""
    assert extract_study_id_from_path(path) == identifier

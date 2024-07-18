from ot_orchestration.utils import GCSPath


def test_gcs_path() -> None:
    path = "gs://some_bucket/some_path/file.txt"
    res = GCSPath(path)
    assert res.bucket == "some_bucket"
    assert res.path == "some_path/file.txt"
    assert res.split() == ("some_bucket", "some_path/file.txt")

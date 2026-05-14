"""Focused Airflow 3 migration tests for the unified pipeline path."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
REPRESENTATIVE_CUSTOM_AIRFLOW_FILES = {
    "src/orchestration/operators/batch/batch_index.py",
    "src/orchestration/operators/batch/batch_index_operator.py",
    "src/orchestration/operators/batch/manifest_generators/gentropy_step.py",
    "src/orchestration/operators/batch/manifest_generators/harmonisation.py",
    "src/orchestration/operators/dataproc.py",
    "src/orchestration/operators/diff.py",
    "src/orchestration/operators/gce.py",
    "src/orchestration/operators/gcs.py",
    "src/orchestration/utils/dataproc.py",
    "src/orchestration/utils/labels.py",
}


def run_fresh_python(script: str) -> subprocess.CompletedProcess[str]:
    """Run a Python snippet in a fresh interpreter inside the repository."""
    return subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        cwd=REPO_ROOT,
        text=True,
    )


def test_unified_pipeline_loads_without_airflow_deprecation_warnings() -> None:
    """The representative DAG should load on Airflow 3 without DAG-level deprecated imports."""
    result = run_fresh_python("""
from pathlib import Path
import json
import warnings

from airflow.models import DagBag

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    dag_bag = DagBag(dag_folder="src/orchestration/dags/unified_pipeline.py", include_examples=False)

dag_warnings = [
    {
        "filename": Path(w.filename).as_posix(),
        "category": w.category.__name__,
        "message": str(w.message),
    }
    for w in caught
    if Path(w.filename).as_posix().endswith("src/orchestration/dags/unified_pipeline.py")
    and "airflow" in str(w.message).lower()
]

assert "unified_pipeline" in dag_bag.dags, dag_bag.import_errors
assert not dag_bag.import_errors, dag_bag.import_errors
assert not dag_warnings, json.dumps(dag_warnings, indent=2)
""")

    assert result.returncode == 0, result.stderr or result.stdout


def test_unified_pipeline_custom_airflow_types_import_without_deprecation_warnings() -> None:
    """Representative custom Airflow integrations should use supported Airflow 3 imports."""
    result = run_fresh_python(f"""
from pathlib import Path
import json
import warnings

from airflow.models import DagBag

TARGETS = {sorted(REPRESENTATIVE_CUSTOM_AIRFLOW_FILES)!r}

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    dag_bag = DagBag(dag_folder="src/orchestration/dags/unified_pipeline.py", include_examples=False)

custom_warnings = [
    {{
        "filename": Path(w.filename).as_posix(),
        "category": w.category.__name__,
        "message": str(w.message),
    }}
    for w in caught
    if any(Path(w.filename).as_posix().endswith(target) for target in TARGETS)
    and "airflow" in str(w.message).lower()
]

assert "unified_pipeline" in dag_bag.dags, dag_bag.import_errors
assert not custom_warnings, json.dumps(custom_warnings, indent=2)
""")

    assert result.returncode == 0, result.stderr or result.stdout

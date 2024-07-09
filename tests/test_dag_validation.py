from airflow.models import DagBag


def test_no_import_errors(dag_bag: DagBag) -> None:
    """Test for import errors."""
    assert (
        not dag_bag.import_errors
    ), f"DAG import failures. Errors: {dag_bag.import_errors}"


def test_requires_tags(dag_bag: DagBag) -> None:
    """Tags should be defined for each DAG."""
    for _, dag in dag_bag.dags.items():
        assert dag.tags


def test_owner_len_greater_than_five(dag_bag: DagBag) -> None:
    """Owner should be defined for each DAG and be longer than 5 characters."""
    for _, dag in dag_bag.dags.items():
        assert len(dag.owner) > 5


def test_desc_len_greater_than_fifteen(dag_bag: DagBag) -> None:
    """Description should be defined for each DAG and be longer than 30 characters."""
    for _, dag in dag_bag.dags.items():
        if isinstance(dag.description, str):
            assert len(dag.description) > 30


def test_owner_not_airflow(dag_bag: DagBag) -> None:
    """Owner should not be 'airflow'."""
    for _, dag in dag_bag.dags.items():
        assert str.lower(dag.owner) != "airflow"


def test_three_or_less_retries(dag_bag: DagBag) -> None:
    """Retries should be 3 or less."""
    for _, dag in dag_bag.dags.items():
        assert dag.default_args["retries"] <= 3

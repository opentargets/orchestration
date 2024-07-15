"""Gwas catalog DAG."""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
import pytest

RUN_DATE = datetime.today()


@pytest.mark.skip(reason="Fails with sqlite error")
def test_task_expanding() -> None:
    @dag(start_date=RUN_DATE, dag_id="test_expand")
    def expand_with_many_processes() -> None:
        """Test expand."""

        @task(task_id="test_expand_task")
        def test() -> list[int]:
            return [n for n in range(1000)]

        @task(task_id="print")
        def print_me(p: int) -> None:
            print(p)

        print_me.partial().expand(p=test())

    expand_with_many_processes().test()

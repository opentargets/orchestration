import pendulum
from airflow.decorators import task
from airflow.models import DAG
from airflow.models.baseoperator import BaseOperator

with DAG(
    "test_dag",
    description="test dag",
    schedule=None,
    start_date=pendulum.datetime(2015, 12, 1, tz="UTC"),
    catchup=False,
) as dag:

    @task(task_id="test_1")
    def test_1():
        print("hello world!")

    test_1()

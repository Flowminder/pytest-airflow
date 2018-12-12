import pytest
import datetime

from airflow import DAG

test_dag = DAG(dag_id="foo", start_date = datetime.datetime(2017, 1, 1))

class MyPlugin:
    @pytest.fixture(scope="session")
    def dag(self):
        print("CUSTOM DAG")
        return test_dag

pytest.main(["--airflow"], plugins=[MyPlugin()])


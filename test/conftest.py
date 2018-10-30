
import pytest
import logging
import datetime


@pytest.fixture(scope="session")
def owner():
    yield "airflow"
    # print("finishing owner")

@pytest.fixture(scope="session")
def dag_default_args(owner):
    print('dag_default_args 1')
    yield {
        "owner": owner,
        "start_date": datetime.datetime(2017, 1, 1),
        "end_date": None,
        "depends_on_past": False,
    }
    print("finishing dag_default_args")

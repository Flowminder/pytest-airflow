import pytest
import datetime
import logging


@pytest.fixture(scope="session")
def dag_default_args():
    print('dag_default_args 2')
    yield {
        "owner": "airflow",
        "start_date": datetime.datetime(2017, 1, 1),
        "depends_on_past": False,
    }
    print("finishing dag_default_args 2")


@pytest.fixture()
def fix_func():
    x = 1
    yield x
    x = 2


@pytest.fixture(scope="module")
def fix_mod():
    y = 5
    yield y
    y = 2


@pytest.fixture(scope="module", params=["blue", "red"])
def fix_color(request):
    yield request.param


def test_bar_1(request, fix_func, fix_mod, fix_color, task_ctx):
    request.addfinalizer(lambda: logging.info("bulldog"))
    logging.info("bar_1")
    logging.info(fix_func)
    logging.info(fix_mod)
    logging.info(fix_color)
    logging.info(task_ctx)
    assert 0


def test_bar_2(fix_func, fix_mod, task_ctx):
    logging.info("bar_2")
    logging.info(fix_func)
    logging.info(fix_mod)
    logging.info(task_ctx)
    assert 1

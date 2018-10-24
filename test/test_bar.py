import pytest
import logging

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

def test_bar_1(fix_func, fix_mod, fix_color, task_ctx):
    logging.info('bar_1')
    logging.info(fix_func)
    logging.info(fix_mod)
    logging.info(fix_color)
    logging.info(task_ctx)
    assert 1

def test_bar_2(fix_func, fix_mod, task_ctx):
    logging.info('bar_2')
    logging.info(fix_func)
    logging.info(fix_mod)
    logging.info(task_ctx)
    assert 1

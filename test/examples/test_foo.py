import pytest
import logging


@pytest.fixture()
def fix_func(defer_fix_bar):
    logging.info("inside fix_func")
    x = 1 + defer_fix_bar
    yield x
    x = 2
    logging.info("deferred final func")


@pytest.fixture()
def defer_fix_bar(request):
    request.addfinalizer(lambda: logging.info("boo"))
    logging.info(request)
    logging.info(request._pyfuncitem.session._setupstate.__dict__)
    logging.info("inside defer_fix_bar")
    x = 1
    yield x
    x = 2
    logging.info("deferred final bar")


@pytest.fixture()
def fix_bleh():
    x = 25
    yield 25
    x = 10
    logging.info("deferred final bleh")


@pytest.fixture()
def defer_bleh(boo):
    return boo


@pytest.fixture()
def boo(task_ctx):
    return task_ctx["dag"]


@pytest.mark.webtest
def test_foo(fix_func, defer_fix_bar, fix_bleh):
    logging.info("here")
    logging.info(fix_func)
    logging.info(defer_fix_bar)
    logging.info(fix_bleh)
    assert 1


@pytest.mark.blue
def test_foo_foo(fix_bleh, defer_bleh):
    logging.info(f"{defer_bleh}")
    assert 1
    logging.info(fix_bleh)


def test_skip(fix_bleh, defer_bleh):
    pytest.skip("This should skip.")
    logging.info(f"{defer_bleh}")
    assert 1
    logging.info(fix_bleh)

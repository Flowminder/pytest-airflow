# -*- coding: utf-8 -*-
""" Test the plugin in the context of a simple test directory structure. """

import pytest
from _pytest._code.code import ExceptionChainRepr
from airflow.models import DAG, BaseOperator


@pytest.fixture()
def simple_testdir(testdir):
    """ Creates a simple test directory. """
    testdir.makepyfile(
        test_foo="""
            def test_succeeds():
                assert 1

            def test_fails():
                assert 0
            """,
        test_bar="""
            def test_succeeds():
                assert 1

            def test_fails():
                assert 0
            """,
    )


def test_returned_tasks(testdir, simple_testdir):
    """Test that all tests are requested tests are returned as tasks. """
    result = testdir.runpytest("--airflow")
    dag, _, _ = result.ret
    tasks = dag.task_dict
    want = set(
        [
            "__pytest_source",
            "__pytest_sink",
            "test_foo.py-test_succeeds",
            "test_foo.py-test_fails",
            "test_bar.py-test_succeeds",
            "test_bar.py-test_fails",
        ]
    )
    got = set(tasks.keys())

    assert want == got

    result = testdir.runpytest("--airflow", "-k", "test_foo")
    dag, _, _ = result.ret
    tasks = dag.task_dict
    want = set(
        [
            "__pytest_source",
            "__pytest_sink",
            "test_foo.py-test_succeeds",
            "test_foo.py-test_fails",
        ]
    )
    got = set(tasks.keys())

    assert want == got


def test_deferred_task_execution(testdir, simple_testdir, mock_context):
    """ Test deferred test execution. """
    result = testdir.runpytest("--airflow")
    dag, _, _ = result.ret
    tasks = dag.task_dict

    tasks["test_foo.py-test_succeeds"].execute(mock_context)
    assert mock_context["ti"].xcom == {"outcome": "passed", "longrepr": None}

    with pytest.raises(AssertionError):
        tasks["test_foo.py-test_fails"].execute(mock_context)
    assert mock_context["ti"].xcom["outcome"] == "failed"
    assert isinstance(mock_context["ti"].xcom["longrepr"], ExceptionChainRepr)

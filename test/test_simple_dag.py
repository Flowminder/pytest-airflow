# -*- coding: utf-8 -*-
"""Test features of the DAG created by the plugin."""

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


def test_vanilla_execution(testdir, simple_testdir):
    """ Test that vanilla execution does not use the plugin. """
    result = testdir.runpytest()
    result.assert_outcomes(failed=2, passed=2)


def test_returned_values(testdir, simple_testdir):
    """ Test pytest-airflow returns values as expected. """
    result = testdir.runpytest("--airflow")
    dag, source, sink = result.ret

    result.assert_outcomes(passed=4)

    assert isinstance(dag, DAG)
    assert isinstance(source, BaseOperator)
    assert isinstance(sink, BaseOperator)

    assert dag.dag_id == "pytest"
    assert source.task_id == "__pytest_source"
    assert sink.task_id == "__pytest_sink"

    tasks = dag.topological_sort()

    assert tasks[0] == source
    assert tasks[-1] == sink


def test_custom_dag_name(testdir, simple_testdir):
    """ Test custom DAG name."""
    result = testdir.runpytest("--airflow", "--dag-id", "foo")
    dag, _, _ = result.ret

    assert dag.dag_id == "foo"


def test_custom_terminating_tasks_name(testdir, simple_testdir):
    """ Test custom source and sink name."""
    result = testdir.runpytest("--airflow", "--source", "foo")
    _, source, _ = result.ret
    assert source.task_id == "foo"

    result = testdir.runpytest("--airflow", "--sink", "foo")
    _, _, sink = result.ret
    assert sink.task_id == "foo"


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

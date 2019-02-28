# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

""" Test the plugin in the context of a test directory structure without fixtures. """

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

    mock_context = mock_context()
    tasks["test_foo.py-test_succeeds"].execute(mock_context)
    assert mock_context["ti"].xcom == {"outcome": "passed", "longrepr": None}

    with pytest.raises(AssertionError):
        tasks["test_foo.py-test_fails"].execute(mock_context)
    assert mock_context["ti"].xcom["outcome"] == "failed"
    assert isinstance(mock_context["ti"].xcom["longrepr"], ExceptionChainRepr)


def test_task_dependencies(testdir, simple_testdir):
    """ Test that all task dependencies are met. """
    result = testdir.runpytest("--airflow")
    dag, _, _ = result.ret

    task_ids = [
        "test_foo.py-test_succeeds",
        "test_foo.py-test_fails",
        "test_bar.py-test_succeeds",
        "test_bar.py-test_fails",
    ]

    downstream = dag.task_dict["__pytest_source"].get_direct_relative_ids(
        upstream=False
    )
    assert set(task_ids) == set(downstream)
    upstream = dag.task_dict["__pytest_source"].get_direct_relative_ids(upstream=True)
    assert set() == set(upstream)

    downstream = dag.task_dict["__pytest_sink"].get_direct_relative_ids(upstream=False)
    assert set() == set(upstream)
    upstream = dag.task_dict["__pytest_sink"].get_direct_relative_ids(upstream=True)
    assert set(task_ids) == set(upstream)

    for id in task_ids:
        task = dag.task_dict[id]
        downstream = task.get_direct_relative_ids(upstream=False)
        assert downstream == set(["__pytest_sink"])
        upstream = task.get_direct_relative_ids(upstream=True)
        assert upstream == set(["__pytest_source"])


def test_skipped(testdir, mock_context):
    """ Test that skipped tasks succeed and are passed to xcom correctly. """
    testdir.makepyfile(
        test_foo="""
            import pytest

            def test_skips():
                pytest.skip("Skip this.")
                assert 1
            """
    )
    result = testdir.runpytest("--airflow")
    dag, _, _ = result.ret

    mock_context = mock_context()
    dag.task_dict["test_foo.py-test_skips"].execute(mock_context)
    assert mock_context["ti"].xcom["outcome"] == "skipped"
    assert isinstance(mock_context["ti"].xcom["longrepr"], ExceptionChainRepr)

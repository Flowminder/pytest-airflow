# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

""" Test cmdline modifications. """

import pytest
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

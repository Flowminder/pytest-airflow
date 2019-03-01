# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

""" Test fixture deferrement. """

import pytest
import logging
from airflow.models import DAG, BaseOperator


def test_not_deferred(testdir, caplog, mock_context):
    """ Test that fixture is not deferred. """
    testdir.makepyfile(
        test_foo="""
        import pytest
        import logging

        @pytest.fixture()
        def not_deferred():
            logging.info("Initializing fixture.")
            yield 1
            logging.info("Finalizing fixture.")

        def test_foo(not_deferred):
            logging.info("Running test.")
            assert not_deferred
        """
    )
    result = testdir.runpytest("--airflow")
    assert caplog.record_tuples == [
        ("root", logging.INFO, "Initializing fixture."),
        ("root", logging.INFO, "Finalizing fixture."),
    ]
    caplog.clear()

    dag, _, _ = result.ret
    dag.task_dict["test_foo.py-test_foo"].execute(mock_context())
    assert caplog.record_tuples == [("root", logging.INFO, "Running test.")]


def test_defer_with_yield(testdir, caplog, mock_context):
    """ Test fixture execution is deferred and properly finalized. """
    testdir.makepyfile(
        test_foo="""
        import pytest
        import logging

        @pytest.fixture()
        def defer_fixture():
            logging.info("Initializing fixture.")
            yield 1
            logging.info("Finalizing fixture.")

        def test_foo(defer_fixture):
            logging.info("Running test.")
            assert defer_fixture
        """
    )
    result = testdir.runpytest("--airflow")
    assert caplog.record_tuples == [("root", logging.INFO, "Deferring defer_fixture.")]
    caplog.clear()

    dag, _, _ = result.ret
    dag.task_dict["test_foo.py-test_foo"].execute(mock_context())
    assert caplog.record_tuples == [
        ("root", logging.INFO, "Executing the deferred call for defer_fixture"),
        ("root", logging.INFO, "Initializing fixture."),
        ("root", logging.INFO, "Running test."),
        ("root", logging.INFO, "Executing the deferred teardown for defer_fixture"),
        ("root", logging.INFO, "Finalizing fixture."),
    ]


def test_defer_with_return(testdir, caplog, mock_context):
    """ Test fixture execution is deferred and properly finalized. """
    testdir.makepyfile(
        test_foo="""
        import pytest
        import logging

        @pytest.fixture()
        def defer_fixture(request):
            logging.info("Initializing fixture.")
            request.addfinalizer(lambda: logging.info("Finalizing fixture."))
            return 1

        def test_foo(defer_fixture):
            logging.info("Running test.")
            assert defer_fixture
        """
    )
    result = testdir.runpytest("--airflow")
    assert caplog.record_tuples == [("root", logging.INFO, "Deferring defer_fixture.")]
    caplog.clear()

    dag, _, _ = result.ret
    dag.task_dict["test_foo.py-test_foo"].execute(mock_context())
    assert caplog.record_tuples == [
        ("root", logging.INFO, "Executing the deferred call for defer_fixture"),
        ("root", logging.INFO, "Initializing fixture."),
        ("root", logging.INFO, "Running test."),
        ("root", logging.INFO, "Executing the deferred teardown for defer_fixture"),
        ("root", logging.INFO, "Finalizing fixture."),
    ]


def test_fixture_depends_on_deferred(testdir, caplog, mock_context):
    """ Test that fixture that depends on a deferred one is also deferred. """
    testdir.makepyfile(
        test_foo="""
        import pytest
        import logging

        @pytest.fixture()
        def defer_fixture():
            yield 1

        @pytest.fixture()
        def simple_fixture(defer_fixture):
            logging.info("Initializing fixture.")
            yield 1
            logging.info("Finalizing fixture.")

        def test_foo(simple_fixture):
            logging.info("Running test.")
            assert simple_fixture
        """
    )
    result = testdir.runpytest("--airflow")
    assert caplog.record_tuples == [
        ("root", logging.INFO, "Deferring defer_fixture."),
        ("root", logging.INFO, "Deferring simple_fixture."),
    ]
    caplog.clear()

    dag, _, _ = result.ret
    dag.task_dict["test_foo.py-test_foo"].execute(mock_context())
    print(caplog.record_tuples)
    assert caplog.record_tuples == [
        ("root", logging.INFO, "Executing the deferred call for simple_fixture"),
        ("root", logging.INFO, "Executing the deferred call for defer_fixture"),
        ("root", logging.INFO, "Initializing fixture."),
        ("root", logging.INFO, "Running test."),
        ("root", logging.INFO, "Executing the deferred teardown for simple_fixture"),
        ("root", logging.INFO, "Finalizing fixture."),
    ]


def test_task_ctx_deferred(testdir, caplog, mock_context):
    """ Test task_ctx execution is deferred and properly finalized. """
    testdir.makepyfile(
        test_foo="""
        import pytest
        import logging

        @pytest.fixture()
        def task_ctx():
            logging.info("Initializing fixture.")
            yield {}
            logging.info("Finalizing fixture.")

        def test_foo(task_ctx):
            logging.info("Running test.")
            assert 1
        """
    )
    result = testdir.runpytest("--airflow")
    assert caplog.record_tuples == [("root", logging.INFO, "Deferring task_ctx.")]
    caplog.clear()

    dag, _, _ = result.ret
    dag.task_dict["test_foo.py-test_foo"].execute(mock_context())
    assert caplog.record_tuples == [
        ("root", logging.INFO, "Executing the deferred call for task_ctx"),
        ("root", logging.INFO, "Initializing fixture."),
        ("root", logging.INFO, "Running test."),
        ("root", logging.INFO, "Executing the deferred teardown for task_ctx"),
        ("root", logging.INFO, "Finalizing fixture."),
    ]

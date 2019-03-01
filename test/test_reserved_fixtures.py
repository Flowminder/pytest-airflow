# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

""" Test reserved fixtures. """

import pytest
import logging
import datetime
from airflow.models import DAG


def test_user_dag(testdir):
    """ Test that plugin selects user DAG. """
    testdir.makepyfile(
        test_foo="""
        import pytest
        import datetime
        from airflow.models import DAG

        @pytest.fixture(scope="session")
        def dag():
            args = {
                "owner": "bob",
                "start_date": datetime.datetime(2017, 1, 1),
                "end_date": None,
                "depends_on_past": False,
            }
            dag = DAG(dag_id="foo", schedule_interval=None, default_args=args)
            return dag

        def test_foo():
            assert 1
        """
    )
    result = testdir.runpytest("--airflow")
    dag, _, _ = result.ret

    assert dag.dag_id == "foo"
    assert dag.owner == "bob"


def test_user_default_args(testdir):
    """ Test that plugin selects user_default_args. """
    testdir.makepyfile(
        test_foo="""
        import pytest
        import datetime
        from airflow.models import DAG

        @pytest.fixture(scope="session")
        def dag_default_args():
            return {
                "owner": "bob",
                "start_date": datetime.datetime(2017, 1, 1),
                "end_date": None,
                "depends_on_past": False,
            }

        def test_foo():
            assert 1
        """
    )
    result = testdir.runpytest("--airflow")
    dag, _, _ = result.ret

    assert dag.owner == "bob"


def test_user_dag_report(testdir, capsys, mock_context):
    """ Test that plugin selects user_default_args. """
    testdir.makepyfile(
        test_foo="""
        import pytest
        import logging
        import datetime
        from airflow.models import DAG

        @pytest.fixture(scope="session")
        def dag_report(**kwargs):
            logging.info("Custom report.")

        def test_foo():
            assert 1
        """
    )
    result = testdir.runpytest("--airflow")
    _, _, sink = result.ret

    sink.execute(mock_context())
    captured = capsys.readouterr()
    assert "Custom report." in captured.out


def test_correct_fixture_selected(testdir):
    """ Test that the correct reserved fixture is selected. """
    testdir.makepyfile(
        test_foo="""
            import pytest
            from airflow.models import DAG

            @pytest.fixture(scope="session")
            def dag(dag_default_args):
                dag = DAG(dag_id="foo", schedule_interval=None,
                    default_args=dag_default_args)
                return dag

            def test_succeeds():
                assert 1
            """,
        test_bar="""
            import pytest
            from airflow.models import DAG

            @pytest.fixture(scope="session")
            def dag(dag_default_args):
                dag = DAG(dag_id="bar", schedule_interval=None,
                    default_args=dag_default_args)
                return dag

            def test_succeeds():
                assert 1
            """,
    )
    testdir.makeconftest(
        """
        import pytest
        from airflow.models import DAG

        @pytest.fixture(scope="session")
        def dag(dag_default_args):
            dag = DAG(dag_id="conftest", schedule_interval=None,
                default_args=dag_default_args)
            return dag
    """
    )
    result = testdir.runpytest("--airflow")
    dag, _, _ = result.ret
    assert dag.dag_id == "conftest"

    result = testdir.runpytest("--airflow", "test_foo.py")
    dag, _, _ = result.ret
    assert dag.dag_id == "foo"


def test_fixture_must_be_session_scoped(testdir):
    """ Test that the fixture not scoped as session is not picked up. """
    testdir.makepyfile(
        test_foo="""
        import pytest
        import logging
        import datetime
        from airflow.models import DAG

        @pytest.fixture()
        def dag_default_args():
            return {
                "owner": "bob",
                "start_date": datetime.datetime(2017, 1, 1),
                "end_date": None,
                "depends_on_past": False,
            }

        def test_foo():
            assert 1
        """
    )
    result = testdir.runpytest("--airflow")
    dag, _, _ = result.ret
    assert dag.owner == "airflow"


def test_fixture_from_plugin(testdir):
    """ Test that fixtures defined in other plugins take precedence. """

    want = DAG(dag_id="foo", start_date=datetime.datetime(2017, 1, 1))

    class MyPlugin:
        @pytest.fixture(scope="session")
        def dag(self):
            return want

    testdir.makepyfile(
        test_foo="""
        def test_foo():
            assert 1
        """
    )
    result = testdir.runpytest("--airflow", plugins=[MyPlugin()])
    got, _, _ = result.ret

    assert want == got


def test_default_dag_report_suceeds_when_all_suceed(testdir, mock_context):
    """ Test that the default report task succeed when all tests succeed. """

    testdir.makepyfile(
        test_foo="""
        def test_succeeds():
            assert 1
        """
    )
    result = testdir.runpytest("--airflow")
    _, _, report = result.ret

    mock_context = mock_context(__pytest_source={"state": "SUCCESS"}, test_succeeds={"state":"SUCCESS"})
    mock_context["ti"].xcom = {"test_succeeds": {"outcome": "passed"}}
    report.execute(mock_context)


def test_default_dag_report_fails_when_any_test_fails(testdir, mock_context):
    """ Test that the default report task fails when at least one test fails. """

    testdir.makepyfile(
        test_foo="""
        def test_succeeds():
            assert 1

        def test_fails():
            assert 0
        """
    )
    result = testdir.runpytest("--airflow")
    _, _, report = result.ret

    mock_context = mock_context(__pytest_source={"state": "SUCCESS"}, test_succeeds={"state": "SUCCESS"}, test_fails={"state": "FAILED"})
    mock_context["ti"].xcom = {
        "test_succeeds": {"outcome": "passed"},
        "test_fails": {"outcome": "failed", "longrepr": "failed"},
    }
    with pytest.raises(Exception):
        report.execute(mock_context)

def test_default_dag_report_reports_non_completed_tasks(testdir, mock_context, caplog):
    """ Test that the default report task logs tasks that did not complete according to Airflow state. """
    testdir.makepyfile(
        test_foo="""
        def test_succeeds():
            assert 1

        def test_skipped_by_airflow():
            assert 0
        """
    )
    result = testdir.runpytest("--airflow")
    _, _, report = result.ret

    mock_context = mock_context(__pytest_source={"state": "SUCCESS"}, test_succeeds={"state": "SUCCESS"}, test_skipped_by_airflow={"state": "SKIPPED"})
    mock_context["ti"].xcom = {
        "test_succeeds": {"outcome": "passed"},
    }
    report.execute(mock_context)
    assert caplog.record_tuples == [
        ("root", logging.INFO, "Test results report."),
        ("root", logging.INFO, "test_succeeds completed, pytest outcome: passed."),
        ("root", logging.INFO, "test_skipped_by_airflow did not complete, task marked as: SKIPPED."),
    ]

def test_default_dag_report_state_follows_source_task_state(testdir, mock_context, caplog):
    """ Test that the default report task state follows the source task state. """
    testdir.makepyfile(
        test_foo="""
        def test_succeeds():
            assert 1

        def test_skipped_by_airflow():
            assert 0
        """
    )
    result = testdir.runpytest("--airflow")
    _, _, report = result.ret

    mock_context = mock_context(__pytest_source={"state": "UPSTREAM_FAILED"}, test_succeeds={"state": "UPSTREAM_FAILED"}, test_skipped_by_airflow={"state": "UPSTREAM_FAILED"})
    with pytest.raises(Exception):
        report.execute(mock_context)




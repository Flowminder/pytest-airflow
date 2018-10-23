"""pytest-airflow implementation."""
import datetime

import pytest
import _pytest.runner as runner

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator


@pytest.fixture(scope="session")
def dag(request):
    """Return the DAG for the current session."""

    if request.config.option.airflow:

        if hasattr(request.config, "dag_id"):
            dag_id = request.config.dag_id
        else:
            dag_id = "pytest"

        # TODO: allow user to define this
        args = {"owner": "airflow", "start_date": datetime.datetime(2018, 1, 1)}

        dag = DAG(dag_id=dag_id, schedule_interval=None, default_args=args)

        return dag


def pytest_addoption(parser):
    group = parser.getgroup("airflow")
    group.addoption("--airflow", action="store_true", help="run tests with airflow.")
    group.addoption("--dag-id", help="set the airflow dag id name.")


@pytest.hookimpl(hookwrapper=True)
def pytest_collection_modifyitems(session, config, items):
    outcome = yield
    if session.config.option.airflow:
        dag = items[0]._request.getfixturevalue("dag")
        branch = BranchPythonOperator(
            task_id="__pytest_branch",
            python_callable=lambda: __pytest_branch_callable(items),
            provide_context=True,
            dag=dag,
        )
        dag << branch


def __pytest_branch_callable(items):
    def __callable(**kwargs):

        tasks = []

        if kwargs["dag_run"].conf["markers"]:
            markers = kwargs["dag_run"]["markers"]
            for item in items:
                for m in markers:
                    if item.get_marker(m):
                        tasks.append(_gen_task_id(item))
                        break
        else:
            for item in items:
                tasks.append(_gen_task_id(item))

        return tasks

    return __callable


@pytest.hookimpl(hookwrapper=True)
def pytest_cmdline_main(config):
    config._dag = None
    outcome = yield
    if config._dag:
        print(config._dag.tree_view())
        return config._dag


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_protocol(item, nextitem):
    if item.session.config.option.airflow:
        # implemented in the same way as in pytest src (src/_pytest/runner.py)
        item.ihook.pytest_runtest_logstart(nodeid=item.nodeid, location=item.location)
        runtestprotocol(item, nextitem=nextitem)
        item.ihook.pytest_runtest_logfinish(nodeid=item.nodeid, location=item.location)
        return True


def runtestprotocol(item, log=True, nextitem=None):
    hasrequest = hasattr(item, "_request")
    if hasrequest and not item._request:
        item._initrequest()
    rep = runner.call_and_report(item, "setup", log)
    reports = [rep]
    if rep.passed:
        if item.config.option.setupshow:
            runner.show_test_item(item)
        if not item.config.option.setuponly:
            reports.append(create_airflow_task(item, "call", log))
            if not nextitem:
                item.config._dag = item._request.getfixturevalue("dag")
    reports.append(runner.call_and_report(item, "teardown", log, nextitem=nextitem))
    # after all teardown hooks have been called
    # want funcargs and request info to go away
    if hasrequest:
        item._request = False
        item.funcargs = None
    return reports


def create_airflow_task(item, when, log=True, **kwds):

    call = runner.CallInfo(
        lambda: _task_gen(item),
        when=when,
        treat_keyboard_interrupt_as_exception=item.config.getvalue("usepdb"),
    )
    hook = item.ihook
    report = hook.pytest_runtest_makereport(item=item, call=call)
    if log:
        hook.pytest_runtest_logreport(report=report)
    if runner.check_interactive_exception(call, report):
        hook.pytest_exception_interact(node=item, call=call, report=report)
    return report


def _task_gen(item, **kwds):
    runner._update_current_test_var(item, "call")
    dag = item._request.getfixturevalue("dag")
    ihook = getattr(item.ihook, "pytest_runtest_call")
    task_id = _gen_task_id(item)
    task = PythonOperator(
        task_id=task_id,
        python_callable=lambda: ihook(item=item, **kwds),
        provide_context=True,
        dag=dag,
    )
    dag.set_dependency(task_id, "__pytest_branch")
    return task


def _gen_task_id(item):
    return item.nodeid.replace("/", "__").replace("::", "___")

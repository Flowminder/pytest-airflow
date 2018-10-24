"""pytest-airflow implementation."""
import datetime

import pytest
import _pytest.runner as runner

from airflow import DAG
from airflow.operators.python_operator import (
    PythonOperator,
    BranchPythonOperator,
    SkipMixin,
)


class MultiBranchPythonOperator(PythonOperator, SkipMixin):
    """
    Follow Multiple Branches...
    """

    def execute(self, context):
        branch = super(MultiBranchPythonOperator, self).execute(context)
        self.log.info("Following branch %s", branch)
        self.log.info("Marking other directly downstream tasks as skipped")

        downstream_tasks = context["task"].downstream_list
        self.log.debug("Downstream task_ids %s", downstream_tasks)

        skip_tasks = [t for t in downstream_tasks if t.task_id not in branch]
        if downstream_tasks:
            self.skip(context["dag_run"], context["ti"].execution_date, skip_tasks)

        self.log.info("Done.")


@pytest.fixture(scope="session")
def dag(request):
    """Return the DAG for the current session."""

    if request.config.option.airflow:

        if hasattr(request.config.option, "dag_id") and request.config.option.dag_id:
            dag_id = request.config.option.dag_id
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
    # TODO: we should return an empty DAG when no items selected for now we
    # just return None
    if session.config.option.airflow and len(items) > 0:
        dag = items[0]._request.getfixturevalue("dag")
        branch = MultiBranchPythonOperator(
            task_id="__pytest_branch",
            python_callable=__pytest_branch_callable(items),
            provide_context=True,
            dag=dag,
        )


def __pytest_branch_callable(items):
    def __callable(**kwargs):

        tasks = []

        # if kwargs["dag_run"].conf["markers"]:
        #     markers = kwargs["dag_run"]["markers"]
        #     for item in items:
        #         for m in markers:
        #             if item.get_marker(m):
        #                 tasks.append(_gen_task_id(item))
        #                 break
        # else:
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
        outcome.force_result(config._dag)


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
        python_callable=lambda **kwargs: ihook(item=item, **kwds, **kwargs),
        provide_context=True,
        dag=dag,
    )
    dag.set_dependency("__pytest_branch", task_id)
    return task


def _gen_task_id(item):
    return item.nodeid.replace("/", "__").replace("::", "___")

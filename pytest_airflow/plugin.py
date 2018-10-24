"""pytest-airflow implementation."""
import sys
import datetime
import logging
import inspect

import pytest
from _pytest._code.code import ExceptionInfo

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
def dag_default_args(request):
    return {"owner": "airflow", "start_date": datetime.datetime(2018, 1, 1)}


@pytest.fixture(scope="session")
def dag(request, dag_default_args):
    """Return the DAG for the current session."""

    if request.config.option.airflow:

        if hasattr(request.config.option, "dag_id") and request.config.option.dag_id:
            dag_id = request.config.option.dag_id
        else:
            dag_id = "pytest"

        dag = DAG(dag_id=dag_id, schedule_interval=None, default_args=dag_default_args)

        return dag


@pytest.fixture(autouse=True)
def task_ctx():
    return {}


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
        session.config._dag = dag
        branch = MultiBranchPythonOperator(
            task_id="__pytest_branch",
            python_callable=__pytest_branch_callable(items),
            provide_context=True,
            dag=dag,
        )
        report = PythonOperator(
            task_id="__pytest_report",
            python_callable=__pytest_report,
            provide_context=True,
            trigger_rule="all_done",
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


def __pytest_report(**kwargs):

    # for now we just log results
    logging.info("Test results")

    for task in kwargs["task"].upstream_list:
        outcome = kwargs["ti"].xcom_pull(task.task_id, key="outcome")
        longrepr = kwargs["ti"].xcom_pull(task.task_id, key="longrepr")
        logging.info(f"{task.task_id}: {outcome}")
        if longrepr:
            logging.info(longrepr)


@pytest.hookimpl(hookwrapper=True)
def pytest_cmdline_main(config):
    config._dag = None
    outcome = yield
    if config._dag:
        print(config._dag.tree_view())
        outcome.force_result(config._dag)


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem):
    if pyfuncitem.session.config.option.airflow:
        testfunction = pyfuncitem.obj
        dag = pyfuncitem._request.getfixturevalue("dag")
        task_id = _gen_task_id(pyfuncitem)
        if pyfuncitem._isyieldedfunction():
            task = PythonOperator(
                task_id=task_id,
                python_callable=_task_callable(testfunction, *pyfuncitem._args),
                provide_context=True,
                dag=dag,
            )
        else:
            funcargs = pyfuncitem.funcargs
            testkwargs = {}
            for arg in pyfuncitem._fixtureinfo.argnames:
                if not arg in ("dag", "dag_default_args"):
                    testkwargs[arg] = funcargs[arg]
            task = PythonOperator(
                task_id=task_id,
                python_callable=_task_callable(testfunction, **testkwargs),
                provide_context=True,
                dag=dag,
            )
        dag.set_dependency("__pytest_branch", task_id)
        dag.set_dependency(task_id, "__pytest_report")
        return True


def _task_callable(testfunction, *testargs, **testkwargs):
    def _callable(**kwargs):
        if "task_ctx" in testkwargs:
            testkwargs["task_ctx"] = kwargs

        excinfo = None
        sys.last_type, sys.last_value, sys.last_traceback = (None, None, None)

        try:
            if len(testargs) > 0:
                testfunction(*testargs)
            else:
                testfunction(**testkwargs)
        except:
            # Store trace info to allow postmortem debugging
            type, value, tb = sys.exc_info()
            tb = tb.tb_next  # Skip *this* frame
            sys.last_type = type
            sys.last_value = value
            sys.last_traceback = tb
            del type, value, tb  # Get rid of these in this frame
            excinfo = ExceptionInfo()

        if not excinfo:
            kwargs["ti"].xcom_push("outcome", "passed")
            kwargs["ti"].xcom_push("longrepr", None)
        else:
            kwargs["ti"].xcom_push("outcome", "failed")
            kwargs["ti"].xcom_push("longrepr", excinfo.getrepr(style="short"))

        if excinfo:
            raise excinfo.value

    return _callable


def _gen_task_id(item):
    replacements = {"/": "..", "::": "-", "[": "-", "]": ""}
    id = item.nodeid
    for k, v in replacements.items():
        id = id.replace(k, v)
    return id

"""pytest-airflow implementation."""
import datetime
import inspect

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
        args = {
            "owner": "airflow", 
            "start_date": datetime.datetime(2018, 1, 1)
        }

        dag = DAG(dag_id=dag_id, schedule_interval=None, default_args=args)

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
def pytest_pyfunc_call(pyfuncitem):
    if pyfuncitem.session.config.option.airflow:
        testfunction = pyfuncitem.obj
        dag = pyfuncitem._request.getfixturevalue("dag")
        task_id = _gen_task_id(pyfuncitem)
        if pyfuncitem._isyieldedfunction():
            task = PythonOperator(
                task_id=task_id,
                python_callable=lambda **kwargs: testfunction(*pyfuncitem._args),
                provide_context=True,
                dag=dag,
            )
        else:
            funcargs = pyfuncitem.funcargs
            testargs = {}
            for arg in pyfuncitem._fixtureinfo.argnames:
                if arg != "dag":
                    testargs[arg] = funcargs[arg]
            print(testargs)
            print(inspect.signature(testfunction))
            task = PythonOperator(
                task_id=task_id,
                python_callable=_task_callable(testfunction, **testargs),
                provide_context=True,
                dag=dag,
            )
        dag.set_dependency("__pytest_branch", task_id)
        return True

def _task_callable(testfunction, **testargs):

    def _callable(**kwargs):
        if "task_ctx" in testargs:
            testargs["task_ctx"] = kwargs
        return testfunction(**testargs)

    return _callable

def _gen_task_id(item):
    replacements = {
        "/": "..",
        "::": "-",
        "[" : "-",
        "]": "",
    }
    id = item.nodeid
    for k, v in replacements.items():
        id = id.replace(k, v)
    return id


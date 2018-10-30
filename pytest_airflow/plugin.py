"""pytest-airflow implementation."""
import sys
import six
import logging
import datetime
import functools

import pytest
import _pytest.nodes as nodes
import _pytest.fixtures as fixtures

from _pytest._code.code import ExceptionInfo
from _pytest.outcomes import TEST_OUTCOME
from _pytest.mark.legacy import matchmark, matchkeyword

from airflow import DAG
from airflow.operators.python_operator import (
    PythonOperator,
    SkipMixin,
)

#
# CMDLINE
# -------
# Modifications to the cmdline, such as adding options and modifying the return
# value of the main function.
#

@pytest.hookimpl()
def pytest_addoption(parser):
    """ Adds the pytest-airflow plugin cmdline options. """
    group = parser.getgroup("airflow")
    group.addoption("--airflow", action="store_true", help="run tests with airflow.")
    group.addoption("--dag-id", help="set the airflow dag id name.")

@pytest.hookimpl(hookwrapper=True)
def pytest_cmdline_main(config):
    """ Modifies the return value of the cmdline such that it returns a DAG. """
    if config.option.airflow:
        # provides a pointer to the DAG generated during the course of the script.
        config._dag = None
        outcome = yield
        outcome.force_result(config._dag)

#
# FIXTURES
# --------
# Plugin fixtures.
#

@pytest.fixture(autouse=True)
def task_ctx():
    """ Returns a dictionary that is updated with the task context, when the
    test is executed in Airflow. """
    return {}

#
# DEFAULTS
# --------
# Fallbacks for when the required DAG fixtures have not been defined.
#

def _dag_default_args():
    """ Return the default_args for a generic Airflow DAG. """
    return { "owner": "airflow",
        "start_date": datetime.datetime(2018, 1, 1),
        "end_date": None,
        "depends_on_past": False,
    }


def _dag(config, dag_default_args):
    """ Returns the default DAG according to the session configuration and the
    dag_default_args.  """
    dag_id = getattr(config.option, "dag_id") or "pytest"
    dag = DAG(dag_id=dag_id, schedule_interval=None, default_args=dag_default_args)
    return dag


def _dag_report(**kwargs):
    """ The default callable for the report task. """

    logging.info("Test results")

    for task in kwargs["task"].upstream_list:
        outcome = kwargs["ti"].xcom_pull(task.task_id, key="outcome")
        longrepr = kwargs["ti"].xcom_pull(task.task_id, key="longrepr")
        logging.info(f"{task.task_id}: {outcome}")
        if longrepr:
            logging.info(longrepr)

#
# DAG INITIALIZATION
# ------------------
# Initializes the DAG after test items have been collected and are made
# available for modification.
#

@pytest.hookimpl(hookwrapper=True)
def pytest_collection_modifyitems(session, config, items):
    """ Initializes the DAG when pytest makes the collected items available for
    modification, before execution. """
    outcome = yield

    # do not do anything if no items have been collected.
    if session.config.option.airflow and len(items) > 0:

        # attempts to Initialize the DAG using user defined fixtures,
        # otherwise it falls back to the defaults defined above.
        dag = _init_dag(session)
        session.config._dag = dag

        # the source task, that will mark tasks to skip depending on the
        # dag_run context configuration values for "markers" and "Keywords"
        branch = MultiBranchPythonOperator(
            task_id="__pytest_branch",
            python_callable=_pytest_branch_callable(items),
            provide_context=True,
            dag=dag,
        )

        # retrieve the dag_report_callable, which can be defined by the user as
        # fixture.
        dag_report_fix = _get_fixture("dag_report", session)
        if dag_report_fix:
            dag_report_callable = dag_report_fix.func
        else:
            dag_report_callable = _dag_report

        # the sink task, that perfoms test reporting.
        report = PythonOperator(
            task_id="__pytest_report",
            python_callable=dag_report_callable,
            provide_context=True,
            trigger_rule="all_done",
            dag=dag,
        )


def _init_dag(session):
    """ Initializes the DAG by resolving the two fixtures (dag_default_args and
    dag) required to set it up.

    In case the user does not any of the two fixtures, it uses the default
    arguments defined above.

    :return py:class:`DAG`: a tuple that contains the DAG
    """

    dag_default_args_fix = _get_fixture("dag_default_args", session)
    dag_fix = _get_fixture("dag", session)

    if not dag_default_args_fix:
        default_args = _dag_default_args()
    else:
        default_args = _compute_fixture_value(dag_default_args_fix, session)

    if not dag_fix:
        dag = _dag(session.config, default_args)
    else:
        dag = _compute_fixture_value(dag_fix, session)

    return dag


def _get_fixture(argname, session):
    """ Get the fixture named after argname from the session. 

    The fixture must be located at a nodeid that must be the parent of all the
    collected test items.  This is so, because the plugin will only construct a
    single DAG, if the user specifies a fixture that covers only part of the
    requested test items, the desired DAG becomes ambiguous since we do not
    know if a given fixture applies to items it was not meant to cover.

    :return: returns an instance of :py:class:`FixtureDef` if fixture found
    otherwise `None`.
    """

    # look for the requested fixturedefs in the fixturemanager
    fixs = session._fixturemanager._arg2fixturedefs.get(argname, [])

    # loop through the reversed list of fixtures (so, from the more narrow
    # location to the more broad), the first fixture that encompasses all the
    # colleted items found and whose scope is "session" is returned
    parent_fix = None
    for fix in fixs[::-1]:
        for item in session.items:
            if not nodes.ischildnode(fix.baseid, item._nodeid):
                parent_fix = None
                break
            parent_fix = fix
        if parent_fix and parent_fix.scope == "session":
            break

    return parent_fix


def _compute_fixture_value(fixturedef, session):
    """ Computes the fixturedef evaluated in the scope of the first item in the
    list of collected items. """

    # since the fixturedef by definition is located above all of the collected
    # items, its evaluation should be the same regardless of the item chosen
    # for scoping.
    request = session.items[0]._request
    request._compute_fixture_value(fixturedef)
    res = fixturedef.cached_result[0]
    # we clean up any resources used by this fixturedef
    session._setupstate._callfinalizers(session)

    return res


class MultiBranchPythonOperator(PythonOperator, SkipMixin):
    """ Follow multiple branches.

    The default :py:class:`BranchPythonOperator` will expect a callable that
    returns a single task, here we want to return a list of multiple tasks that
    will be executed. All the others are marked to skip.
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


def _pytest_branch_callable(items):
    """ Generates the callable for the MultiBranchPythonOperator taking into
    account the list of collected items. """
    def _callable(**kwargs):

        # we copy the items, as the list is modified by `pytest` throughout the
        # script life cycle.
        _items = items.copy()

        markerexprs = []
        keywordexprs = []

        # the user can pass additional markers and keywrods to have the branch
        # operator filtering tasks to be executed and others to be skiped.
        if kwargs["dag_run"] and kwargs["dag_run"].conf:
            markerexprs = kwargs["dag_run"].conf.get("markers", [])
            keywordexprs = kwargs["dag_run"].conf.get("keywords", [])

        # ensure list
        if not isinstance(markerexprs, list):
            markerexprs = [markerexprs]
        if not isinstance(keywordexprs, list):
            keywordexprs = [keywordexprs]

        logging.info(f"Markers: {markerexprs}")
        logging.info(f"Keywords: {keywordexprs}")

        # multiple markers/keywords are evaluated with the logical OR, eg.
        # mark1 or mark2 or mark3
        # on the other hand, the set of makers and keywords are evaluated with
        # the logical AND, eg. [markers] AND [keywords]
        # thus, for a test to be selected it must match any of the marker
        # expressions and any of the keyword expressions.
        selected = set()
        if markerexprs or keywordexprs:
            if _items:
                for m in markerexprs:
                    selected = selected.union(_select_by_mark(_items, m))
                _items = list(selected)
                for k in keywordexprs:
                    selected = selected.union(_select_by_keyword(_items, k))
        else:
            selected = set(_items)

        # generate the DAG task ids, as only these are recognized by the DAG as
        # valid IDs.
        tasks = []
        for i in selected:
            tasks.append(_gen_task_id(i))

        return tasks

    return _callable


def _select_by_mark(items, markexpr):
    """ Filter remaining items based on markexpr. """
    # this code is based on the pytest src code located in
    # src/_pytest/mark/legacy.py::deselect_by_mark
    remaining = []
    for item in items:
        if matchmark(item, markexpr):
            remaining.append(item)
    return remaining


def _select_by_keyword(items, keywordexpr):
    """ Filter remaining items based on keywordexpr. """
    # this code is based on the pytest src code located in
    # src/_pytest/mark/legacy.py::deselect_by_keyword
    if keywordexpr.startswith("-"):
        keywordexpr = "not " + keywordexpr[1:]
    selectuntil = False
    if keywordexpr[-1:] == ":":
        selectuntil = True
        keywordexpr = keywordexpr[:-1]

    remaining = []
    for item in items:
        if not keywordexpr or matchkeyword(item, keywordexpr):
            if selectuntil:
                keywordexpr = None
            remaining.append(item)

    return remaining


#
# TEST RUNNER
# -----------
# Prepares the collected test items as callables and creates associated Airflow
# tasks. Also, takes care of deferred fixtures.
#


@pytest.hookimpl(tryfirst=True)
def pytest_fixture_setup(fixturedef, request):
    """ Defer fixture execution when running Airflow if fixture name starts
    with `defer_`. """
    # this code is based on the pytest src own implementation of
    # pytest_fixture_setup located in `src/_pytest/fixtures`
    if fixturedef._fixturemanager.config.option.airflow and _defer(fixturedef, request):
        logging.info(f"Deferring {fixturedef.argname}.")

        # we want to save all kwargs in the fixture's signature for deferred
        # execution
        kwargs = {}
        for argname in fixturedef.argnames:
            fixdef = request._get_active_fixturedef(argname)
            result, arg_cache_key, exc = fixdef.cached_result
            request._check_scope(argname, request.scope, fixdef.scope)
            kwargs[argname] = result

        # instead of evaluating the fixture during setup we defer it for later,
        # storing all the required pointers in a convenient class.
        my_cache_key = request.param_index
        deferred_call = FixtureDeferredCall(fixturedef, request, kwargs)
        fixturedef.cached_result = (deferred_call, my_cache_key, None)

        return deferred_call

def _defer(fixturedef, request):
    """ Determines whether fixturedef should be deferred, based on whether it
    starts with `defer_`. """
    if isinstance(fixturedef, fixtures.PseudoFixtureDef):
        return False
    # we perform a recursive call to _defer in order to check if the fixture or
    # any of its dependencies need to be deferred in which case we defer the
    # fixture itself.
    if fixturedef.argname.startswith("defer_"):
        return True
    for argname in fixturedef.argnames:
        fixdef = request._get_active_fixturedef(argname)
        if _defer(fixdef, request):
            return True
    return False


class FixtureDeferredCall:

    def __init__(self, fixturedef, request, kwargs):
        """ Convenient class for storing required pointers for fixture deferred
        exeution.

        :arg fixturedef: fixturedef whose call will be deferred.
        :arg request: the request scope from the calling test item.
        :arg kwargs: fixturedef arguments.
        """
        self.fixturefunc = fixtures.resolve_fixture_function(fixturedef, request)
        self.kwargs = kwargs
        self.argname = fixturedef.argname
        self._cached = False
        self._res = None
        self._finalizers = fixturedef._finalizers.copy()
        # since the fixture has been deferred, we don't actually need or want
        # to perfom any fixture teardown at this point
        fixturedef._finalizers = []

    def execute(self):

        if not self._cached:

            logging.info(f"Executing the deferred call for {self.argname}")

            # we recursively execute deferred calls, results are cached for
            # optimal performance.
            for k, arg in self.kwargs.items():
                if isinstance(arg, FixtureDeferredCall):
                    self.kwargs[k] = arg.execute()

            # this code is based on the pytest src code located in
            # src/_pytest/fixtures.py::call_fixture_func
            yieldctx = fixtures.is_generator(self.fixturefunc)
            try:
                if yieldctx:
                    it = self.fixturefunc(**self.kwargs)
                    self._res = next(it)
                    finalizer = functools.partial(
                        fixtures._teardown_yield_fixture, self.fixturefunc, it
                    )
                    self._finalizers.append(finalizer)
                else:
                    self._res = self.fixturefunc(**self.kwargs)
            except TEST_OUTCOME:
                raise

            self._cached = True

        return self._res

    def finish(self):
        # this code is based on the pytest src code located in
        # src/_pytest/fixtures.py::FixtureDef::finish
        logging.info(f"Executing the deferred teardown for {self.argname}")
        exceptions = []
        try:
            while self._finalizers:
                try:
                    func = self._finalizers.pop()
                    func()
                except:
                    exceptions.append(sys.exc_info())
            if exceptions:
                e = exceptions[0]
                del exceptions
                six.reraise(*e)
        finally:
            self._finalizers = []


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem):
    """ Prepares the test item callable for deferred execution when running Airflow. """
    # this code is based on the pytest src code located in
    # src/_pytest/python.py::pytest_pyfunc_call
    if pyfuncitem.session.config.option.airflow:
        dag = pyfuncitem.session.config._dag
        task_id = _gen_task_id(pyfuncitem)
        if pyfuncitem._isyieldedfunction():
            # in pytest we would call the actual test function at this
            # location, instead we create an Airflow task.
            task = PythonOperator(
                task_id=task_id,
                python_callable=_task_callable(pyfuncitem, *pyfuncitem._args),
                provide_context=True,
                dag=dag,
            )
        else:
            funcargs = pyfuncitem.funcargs
            testkwargs = {}
            for arg in pyfuncitem._fixtureinfo.argnames:
                # we ignore these fixtures which are only used for initializing
                # the DAG and should not be used during test execution.
                if not arg in ("dag", "dag_default_args", "dag_report"):
                    testkwargs[arg] = funcargs[arg]
            # in pytest we would call the actual test function at this
            # location, instead we create an Airflow task.
            task = PythonOperator(
                task_id=task_id,
                python_callable=_task_callable(pyfuncitem, **testkwargs),
                provide_context=True,
                dag=dag,
            )
        # set task dependencies to the branching and reporting tasks upstream
        # and downstream respectively.
        dag.set_dependency("__pytest_branch", task_id)
        dag.set_dependency(task_id, "__pytest_report")
        return True


def _task_callable(pyfuncitem, *testargs, **testkwargs):
    """ Prepares the PythonOperator callable based on the test function item. """
    def _callable(**kwargs):

        # update the task_ctx with the dag_run context
        if "task_ctx" in testkwargs:
            testkwargs["task_ctx"].update(kwargs)

        # performs the deferred fixture calls, updates the testkwargs and
        # registers the associated finalizers for posterior terdown.
        finalizers = []
        for k, arg in testkwargs.items():
            if isinstance(arg, FixtureDeferredCall):
                testkwargs[k] = arg.execute()
                finalizers.append(arg.finish)

        # retrieves the test function
        testfunction = pyfuncitem.obj

        # register any additional finalizers
        if hasattr(pyfuncitem, "teardown"):
            finalizers.append(pyfuncitem.teardown)

        excinfo = None
        exceptions = []
        sys.last_type, sys.last_value, sys.last_traceback = (None, None, None)

        # executes the test function in a controlled environment, collecting
        # information about exceptions for latter reporting.
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
            exceptions.append(ExceptionInfo())

        # communicate test outcomes to the xcom channel, making it accessible
        # to the reporting task downstream
        if exceptions:
            kwargs["ti"].xcom_push("outcome", "failed")
            kwargs["ti"].xcom_push("longrepr", exceptions[0].getrepr(style="short"))
        else:
            kwargs["ti"].xcom_push("outcome", "passed")
            kwargs["ti"].xcom_push("longrepr", None)

        # execute any registered finalizers in a controlled environment either
        # from the deferred fixtures or from the test function itself.
        try:
            while finalizers:
                try:
                    func = finalizers.pop()
                    func()
                except:
                    exceptions.append(sys.exc_info())
        except:
            pass

        # after attempting to perform all required finalization, raise the
        # first exception if any from the exceptions stack.
        if exceptions:
            e = exceptions[0]
            del exceptions
            if isinstance(e, ExceptionInfo):
                six.reraise(e.type, e.value, e.tb)
            else:
                six.reraise(*e)

    return _callable


def _gen_task_id(item):
    """ Translates pytest nodeids to Airflow task ids. """
    replacements = {"/": "..", "::": "-", "[": "-", "]": ""}
    id = item.nodeid
    for k, v in replacements.items():
        id = id.replace(k, v)
    return id

#
# TERMINAL SUMMARY
# ----------------
# Adds plugin specific info to the terminal summary report.
#

@pytest.hookimpl()
def pytest_terminal_summary(terminalreporter, exitstatus):
    """ Reports compiled DAG tree view to terminal during terminal summary
    phase at the end, after test execution. """
    if terminalreporter.config.option.airflow:
        dag = terminalreporter.config._dag
        if dag:
            terminalreporter.write_line(f"DAG: {dag.dag_id}")
            for t in dag.roots:
                _branch_repr(terminalreporter, t)
            terminalreporter.write_line("")
        else:
            terminalreporter.write_line("Didn't get DAG.")


def _branch_repr(terminalreporter, task, level=1):
    """ Recursively writes the DAG treeview to the terminalreporter. """
    repr = (" " * level * 4) + f"{task}"
    terminalreporter.write_line(repr)
    level += 1
    for t in task.upstream_list:
        _branch_repr(terminalreporter, t, level)

pytest-airflow: pytest support for airflow
==========================================

``pytest-airflow`` is a plugin for ``pytest`` that allows tests to be run
within an Airflow DAG.

``pytest`` handles test discovery and function encapsulation, allowing
test declaration to operate in the usual way with the use of
parametrization, fixtures and marks. The generated test callables tests
are eventually passed to ``PythonOperators`` that are run as separate
Airflow tasks.

Usage
-----

When running pytest from the command line, the plugin will collect the
tests and construct the DAG. It will output a DAG tree view in addition to
the requested output.

.. code-block:: bash

        $ pytest --airflow

When invoking pytest from python code, ``pytest.main()`` will
return a reference to the DAG.

.. code-block:: python

        import pytest
        dag = pytest.main(["--airflow", "--dag-id", "FOO"])

If the plugin is installed, ``pytest`` will automatically use it. Saving
the script above in one's DAG folder is enough to trigger the DAG. Note
that ``pytest`` will be evaluated from the path where the Airflow
scheduler is invoked.

Plugin
------

The plugin creates a DAG of the form ``branch -> tests -> report``,
``branch`` marks tests that will be executed and skipped, ``tests``
executes the selected tests as separate tasks and ``report`` reports test
outcome.

Branching
~~~~~~~~~

Airflow requires that any DAG be completely defined before it is run. So
by the nature of Airflow, we cannot use ``pytest`` to collect tests on the
fly based on the results of ``branch``. Rather, ``pytest`` is used to
generate the set of all possible desired tests before ``branch`` is
evaluated. The user can use all of the available flags to ``pytest`` (eg.
``-m``, ``-k``, paths) to narrow the set of initial desired tests down.

The plugin makes a source task called ``branch`` available. This task
allows skipping unwanted tests for a particular DAG run using the
following configuration keys:

* ``marks``: a list of marks, it filters tests in the same way as the
  ``-m`` flag operates when collecting tests with ``pytest``.

* ``keywords``: a list of keywords, it filters tests in the same way as
  the ``-k`` flag operates when collecting tests with ``pytest``.

Fixtures
~~~~~~~~

The plugin defers test execution for the DAG run. That means when calling
``pytest``, the tests will be collected and the associated callables will
be generated and passed to the ``PythonOperator``. If the DAG is compiled
without any errors, ``pytest`` will return the DAG and will exit
sucessfully. That means that it will report that all tests passed, which
only means that the DAG was compiled without any problems.

Fixture setup and teardown are executed at the moment of DAG compilation.
That means that fixtures such as database connections will not be
available at the moment of test execution during a DAG run.

In order to get around this problem there are two alternatives. The first
alternative is to implement a fixture as a factory, and handling fixture
teardown on the test itself.

Alternatively, the plugin allows deferred fixture setup and teardown. In
order to achieve deferred execution, the name of the fixture must be
prefixed with ``defer_`` or it must depend on the reserved fixture
``task_ctx``. That means that the plugin defer the execution of such
fixtures until the DAG is run. Fixtures that depend on a deferred fixture
will also have its execution deferred for later.

The reserved fixture ``task_ctx`` is always deferred. This fixture
evaluates the Airflow task context and is available to the user when
writting tests. Using this fixture, the user has access to all the items
that would be available to ``kwargs`` when setting ``provide_context`` to
``True`` when using the ``PythonOperator`` in Airflow.

All in all, collection time fixture execution should be used for test
parametrization, for generating expensive resources that can be made
available to tests as copies and for generating fixture factories. On the
other hand, deferred fixtures are great for database connections and other
resources that need to be recycled at each test execution.

Reporting
~~~~~~~~~

Finally, the sink task ``report`` can be used for reporting purposes and for
communicating test results to other DAGs using the ``xcom`` channel.  The user
can supply its own ``dag_report`` fixture for customizing its reporting
requirements. The plugin expects the following fixture signature, scoped at the
``session`` level.

.. code-block:: python

        @pytest.fixture(scope="session")
        def dag_report(**kwargs):
          ...


DAG Configuration
~~~~~~~~~~~~~~~~~

The user can configure the DAG using two reserved fixtures for this. The
fixtures must be scoped at the ``session`` level and its location should cover
all the collected test items. The most narrow fixture that covers all of the
collected items will be selected. Otherwise, the plugin uses default values for
those fixtures. Apart from that, fixture execution and discovery should operate
in the usual way.

The first fixture is ``dag_default_args``, which should return
a dictionary with ``default_args`` that will be passed to the dag
initialization. The default returns

.. code-block:: python

      { "owner": "airflow",
        "start_date": datetime.datetime(2018, 1, 1),
        "end_date": None,
        "depends_on_past": False,
      }

The second fixture is ``dag`` which should return an Airflow DAG that will
be used throughout the script.

If the user desires only to modify the name of the DAG, it is possible to
simply pass the ``--dag-id`` flag to the ``pytest`` cmdline.

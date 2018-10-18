pytest-airflow: pytest support for airflow
==========================================

pytest-airflow is a plugin for pytest that allows test to be run in terms of
Airflow DAGs.

pytest handles test discovery and function encapsulation, allowing test
declaration to operate in the usual way with the use of parametrization and
marks. The plugin handles each test callable to separate and independent
PythonOperators. The plugin creates a DAG such that all the tests depend on a
PythonBranchOperator which determines which tests are allowed to run. Test
results are collected by a sink operator that reads test results using airflow
xcom.

Usage
-----

When running pytest from the command line, the plugin will construct and run
the test DAG, with:


.. code-block:: bash

        $ pytest --airflow

When invoking pytest from python code, `pytest.main()` will
return a reference to the DAG without running it.

.. code-block:: python

        dag = pytest.main(["--airflow", "--dag-id", "FOO"])

Any further arguments that should be passed to the DAG initialization should
be defined as paramters at ``conftest.py``

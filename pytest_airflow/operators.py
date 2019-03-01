# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""Custom airflow operators.

Airflow should only be imported when required in order to avoid raising runtime
errors when the user has not setup its Airflow environment and wants to use
pytest without this plugin.
"""
from airflow.operators.python_operator import PythonOperator, SkipMixin


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

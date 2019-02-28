# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from collections import defaultdict

pytest_plugins = ["pytester"]


class MockClass:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class MockTaskInstance:
    """ This class mocks the Airflow TaskInstance class with the objective of
    simulating xcom communication. """

    def __init__(self, state, **kwargs):
        self.xcom = {}
        self.state = state
        self.__dict__.update(kwargs)

    def xcom_push(self, key, val):
        self.xcom[key] = val

    def xcom_pull(self, task_id, key):
        return self.xcom.get(task_id, {}).get(key, None)

    def current_state(self):
        return self.state

@pytest.fixture()
def mock_object():
    """ Creates an instance of a mock object which allows for arbitrary attributes. """
    return lambda **kwargs: MockClass(**kwargs)

@pytest.fixture()
def mock_dag_run(mock_object):
    """ Creates a mock dag_run. """
    def _mock_dag_run(**task_ids):
        """ task_id is a dictionary with task instance attributes.  """
        task_instances = {}
        for task_id, attr in task_ids.items():
            task_instances[task_id] = MockTaskInstance(**attr)
        return mock_object(
            get_task_instance=lambda id: task_instances[id],
            run_id="foo"
        )

    return _mock_dag_run


@pytest.fixture()
def mock_context(mock_object, mock_dag_run):
    """ Creates a mock context for running deferred tests and fixtures. """
    def _mock_context(**task_ids):
        ctx = defaultdict(MockClass)
        ctx["ti"] = MockTaskInstance(state="RUNNING")
        ctx["dag_run"] = mock_dag_run(**task_ids)
        if "__pytest_source" in task_ids:
            task_ids.pop("__pytest_source")
            ctx["task"].upstream_list = [
                mock_object(task_id=id, upstream_list=[mock_object(task_id="__pytest_source")])
                for id in task_ids
            ]
        else:
            ctx["task"].upstream_list = [mock_object(task_id="__pytest_source")]
        return ctx

    return _mock_context



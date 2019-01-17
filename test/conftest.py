# -*- coding: utf-8 -*-
import pytest

from collections import defaultdict

pytest_plugins = ["pytester"]


class MockClass:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class MockTaskInstance:
    """ This class mocks the Airflow TaskInstance class with the objective of
    simulating xcom communication. """

    def __init__(self):
        self.xcom = {}

    def xcom_push(self, key, val):
        self.xcom[key] = val

    def xcom_pull(self, task_id, key):
        return self.xcom.get(task_id, {}).get(key, None)


@pytest.fixture()
def mock_context():
    """ Creates a mock context for running deferred tests and fixtures. """
    ctx = defaultdict(MockClass)
    ctx["ti"] = MockTaskInstance()
    return ctx


@pytest.fixture()
def mock_object():
    """ Creates an instance a mock object which allows for arbitrary attributes. """
    return lambda **kwargs: MockClass(**kwargs)

# -*- coding: utf-8 -*-
import pytest

pytest_plugins = ["pytester"]


class MockTaskInstance:
    """ This class mocks the Airflow TaskInstance class with the objective of
    simulating xcom communication. """

    def __init__(self):
        self.xcom = {}

    def xcom_push(self, key, val):
        self.xcom[key] = val


@pytest.fixture()
def mock_context():
    """ Creates a mock context for running deferred tests and fixtures. """
    return {"ti": MockTaskInstance()}

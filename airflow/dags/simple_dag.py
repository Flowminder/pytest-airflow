# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
import datetime as dt
from airflow import DAG

default_args = {
    "owner": "airflow",
    "start_date": dt.datetime(2018, 2, 12),
}

with DAG(dag_id="pytest", schedule_interval=None, default_args=default_args) as dag:

    class Fixtures:
        @pytest.fixture(scope="session")
        def dag(self):
            return dag

    _, source, report = pytest.main(["--airflow"], plugins=[Fixtures()])


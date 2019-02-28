# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from setuptools import setup, find_packages

setup(
    name="pytest-airflow",
    version="0.0.1",
    author="Guilherme Zagatti",
    author_email="guilherme.zagatti@flowminder.org",
    description="pytest support for airflow.",
    long_description=open("README.rst").read(),
    packages=find_packages(exclude=["test"]),
    python_requires=">=3.5",
    install_requires=["pytest>=4.1.0", "apache-airflow>=1.8.0"],
    entry_points={"pytest11": ["pytest-airflow=pytest_airflow.plugin"]},
)

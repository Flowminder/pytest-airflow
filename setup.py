# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from setuptools import setup, find_packages

import versioneer

setup(
    name="pytest-airflow",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="Guilherme Zagatti",
    author_email="guilherme.zagatti@flowminder.org",
    url="https://github.com/Flowminder/pytest-airflow",
    description="pytest support for airflow.",
    long_description=open("README.rst").read(),
    packages=find_packages(exclude=["test"]),
    python_requires=">=3.5",
    install_requires=["pytest>=4.1.0", "apache-airflow>=1.8.0"],
    entry_points={"pytest11": ["pytest-airflow=pytest_airflow.plugin"]},
    classifiers=[
        "Framework :: Pytest",
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Natural Language :: English",
    ],
)

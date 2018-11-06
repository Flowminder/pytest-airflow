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
    install_requires=["pytest>=3.0.0"],
    entry_points={"pytest11": ["airflow=pytest_airflow.plugin"]},
)

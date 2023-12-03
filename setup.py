#!/usr/bin/env python

from setuptools import setup, find_namespace_packages

setup(
    name="mETL",
    description="A small, basic ETL library for local processing",
    version="0.0.1",
    url="https://github.com/sebmartin/mETL",
    packages=find_namespace_packages(exclude=["tests.*"]),
    package_data={"": ["*.yml"]},
    entry_points={
        "console_scripts": ["metl-run=metl.runner:main"],
    },
    install_requires=[
        "PyYAML>=5.1",
        "simplejson>=3.16.0",
        "urllib3>=1.24.2",
        "psycopg2-binary>=2.8.3",
        "requests",
        "pydantic>=2.0",
    ],
    tests_require=["requests-mock"],
)

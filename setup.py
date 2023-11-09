#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from setuptools import setup, find_packages

tests_require = [
    'mock>=3.0.4,<=5.0.0',
    'pytest-timeout>=1.3.4,<=2.0.2',
    'moto>=2.2.15,<=2.2.20',
    'responses>=0.16.0,<=0.18.0',
    'scipy>=1.3.3,<=1.9.2',
    'pyspark>=2.2.3,<=3.2.4',
    'pytest>=5.3.1,<=6.2.4',
    'pytest-cov>2.6.0,<=2.8.0',
    'flake8>3.8.3,<=3.9.1',
]

setup(
    name='python_moztelemetry',
    # use_scm_version=True,
    author='Roberto Agostino Vitillo',
    author_email='rvitillo@mozilla.com',
    description='Spark bindings for Mozilla Telemetry',
    url='https://github.com/mozilla/python_moztelemetry',
    packages=find_packages(),
    package_dir={'moztelemetry': 'moztelemetry'},
    install_requires=[
        'boto<=2.49.0',
        'boto3<=1.28.57',
        'ujson>=3.2.0,<=5.5.0',
        'requests>2.24.0,<=2.31.0',
        'protobuf>=3.17.3,<=3.19.6',
        'six==1.12',
        'expiringdict==1.2.2',
        "functools32;python_version<'3'",
        'py4j>=0.10.9.4,<=0.10.9.7',
        'pandas>=0.19.2',
        'numpy>=1.18.5,<=1.21.6',
        'PyYAML==5.1.2',
        'python-snappy>=0.5.4,<=0.6.0',
        'urllib3<1.27,>=1.25.4',
        'typed-ast<1.5,>=1.4.0'
    ],
    setup_requires=[
        'pytest-runner>=5.2,<=5.3.2',
        'setuptools_scm>=4.1.2,<=7.0.5'
    ],
    extras_require={
        'testing': tests_require,
    },
    tests_require=tests_require,
)

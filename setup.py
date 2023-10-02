#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from setuptools import setup, find_packages

tests_require = ['mock', 'pytest-timeout', 'moto', 'responses',
                 'scipy', 'pyspark', 'pytest', 'pytest-cov']

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
        'ujson<=5.8.0',
        'requests>2.30.0,<=2.31.0',
        'protobuf>=3.6.0',
        'six==1.12',
        'expiringdict==1.2.2',
        "functools32;python_version<'3'",
        'py4j==0.10.9.7',
        'pandas>=0.19.2',
        'numpy>=1.8.2',
        'PyYAML==5.1.2',
        'python-snappy==0.6.1',
        'urllib3<1.27,>=1.25.4',
        'typed-ast<1.5,>=1.4.0'
    ],
    setup_requires=['pytest-runner', 'setuptools_scm==7.1.0'],
    extras_require={
        'testing': tests_require,
    },
    tests_require=tests_require,
)

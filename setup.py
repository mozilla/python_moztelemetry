#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from setuptools import setup, find_packages

setup(
    name='python_moztelemetry',
    use_scm_version=True,
    author='Roberto Agostino Vitillo',
    author_email='rvitillo@mozilla.com',
    description='Spark bindings for Mozilla Telemetry',
    url='https://github.com/mozilla/python_moztelemetry',
    packages=find_packages(),
    package_dir={'moztelemetry': 'moztelemetry'},
    install_requires=['boto', 'boto3', 'ujson', 'requests', 'protobuf', 'six',
                      'expiringdict', "functools32;python_version<'3'", 'py4j',
                      'pandas>=0.14.1', 'numpy>=1.8.2',
                      'PyYAML', 'python-snappy'],
    setup_requires=['pytest-runner', 'setuptools_scm'],
    extras_require={
        'test': ['mock', 'pytest-timeout', 'moto<=1.1.22', 'responses',
                 'scipy', 'pytest-flake8', 'pytest', 'pytest-cov'],
    },
    tests_require=['python_moztelemetry[test]'],
)

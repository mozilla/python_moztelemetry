#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from setuptools import setup


setup(
    name='python_moztelemetry',
    use_scm_version=True,
    author='Roberto Agostino Vitillo',
    author_email='rvitillo@mozilla.com',
    description='Spark bindings for Mozilla Telemetry',
    url='https://github.com/mozilla/python_moztelemetry',
    packages=['moztelemetry'],
    package_dir={'moztelemetry': 'moztelemetry'},
    install_requires=['boto', 'boto3', 'ujson', 'requests', 'protobuf',
                      'expiringdict', 'functools32', 'futures', 'py4j',
                      'pandas>=0.14.1', 'numpy>=1.8.2', 'joblib',
                      'telemetry-tools', 'findspark', 'happybase'],
    setup_requires=['pytest-runner', 'setuptools_scm'],
    tests_require=['mock', 'pytest', 'moto'],
)

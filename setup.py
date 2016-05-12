#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from setuptools import setup


setup(
    name='python_moztelemetry',
    version='0.3.9.11',
    author='Roberto Agostino Vitillo',
    author_email='rvitillo@mozilla.com',
    description='Spark bindings for Mozilla Telemetry',
    url='https://github.com/vitillo/python_moztelemetry',
    packages=['moztelemetry'],
    package_dir={'moztelemetry': 'moztelemetry'},
    install_requires=['boto', 'backports.lzma', 'ujson', 'requests', 'protobuf', 'expiringdict', 'functools32', 'py4j', 'pandas>=0.14.1', 'numpy>=1.8.2', 'joblib', 'telemetry-tools'],
    setup_requires = ['pytest-runner'],
    tests_require = ['mock', 'pytest'],
)

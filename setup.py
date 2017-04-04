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
    install_requires=['boto', 'boto3', 'ujson', 'requests', 'protobuf==3.1.0',
                      'expiringdict', 'functools32', 'futures', 'py4j',
                      'pandas>=0.14.1', 'numpy>=1.8.2', 'findspark',
                      'happybase>=1.1.0', 'PyYAML', 'python-snappy'],
    setup_requires=['pytest-runner', 'setuptools_scm'],
    # put pytest last to workaround this bug
    # https://bitbucket.org/pypa/setuptools/issues/196/tests_require-pytest-pytest-cov-breaks
    tests_require=['mock', 'pytest-timeout', 'moto', 'responses', 'pytest'],
)

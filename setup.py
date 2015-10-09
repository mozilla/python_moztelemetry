#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from distutils.core import setup

import urllib
import setuptools.command.install


class FetchExternal(setuptools.command.install.install):
    def run(self):
        urllib.urlretrieve("https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/histogram_tools.py", "moztelemetry/histogram_tools.py")
        setuptools.command.install.install.run(self)

setup(cmdclass={'install': FetchExternal},
      name='python_moztelemetry',
      version='0.3.7.0',
      author='Roberto Agostino Vitillo',
      author_email='rvitillo@mozilla.com',
      description='Spark bindings for Mozilla Telemetry',
      url='https://github.com/vitillo/python_moztelemetry',
      packages=['moztelemetry'],
      package_dir={'moztelemetry': 'moztelemetry'},
      install_requires=['boto', 'backports.lzma', 'ujson', 'requests', 'protobuf', 'expiringdict', 'functools32', 'py4j', 'pandas>=0.14.1', 'numpy>=1.8.2', 'joblib', 'telemetry-tools'])

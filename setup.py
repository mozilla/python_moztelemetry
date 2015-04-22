#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from distutils.core import setup

import wget
import setuptools.command.sdist

class FetchExternal(setuptools.command.sdist.sdist):
  def run(self):
    wget.download("https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/histogram_tools.py", out="moztelemetry/histogram_tools.py")
    setuptools.command.sdist.sdist.run(self)

setup(cmdclass={'sdist': FetchExternal},
      name='python_moztelemetry',
      version='0.3.1.2',
      author='Roberto Agostino Vitillo',
      author_email='rvitillo@mozilla.com',
      description='Spark bindings for Mozilla Telemetry',
      url='https://github.com/vitillo/python_moztelemetry',
      packages=['moztelemetry'],
      package_dir={'moztelemetry': 'moztelemetry'},
      install_requires=['boto', 'ujson', 'requests', 'pandas>=0.15.2', 'numpy>=1.9.2', 'telemetry-tools'])

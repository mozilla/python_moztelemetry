
# python_moztelemetry [![Build Status](https://travis-ci.org/mozilla/python_moztelemetry.svg?branch=master)](https://travis-ci.org/mozilla/python_moztelemetry) [![Documentation Status](http://readthedocs.org/projects/python_moztelemetry/badge/?version=latest)](https://python_moztelemetry.readthedocs.io/?badge=latest) [![Updates](https://pyup.io/repos/github/mozilla/python_moztelemetry/shield.svg)](https://pyup.io/repos/github/mozilla/python_moztelemetry/)

Spark bindings for Mozilla Telemetry

## Deploying a code change
After having your PR reviewed and merged create a new release on [github](https://help.github.com/articles/creating-releases/).
A new pypi release will be automatically triggered by Travis.

## Installing from pypi
- To install this package from pypi run:
```
pip install python_moztelemetry
```

## Updating parse_histograms.py
moztelemetry/parse_histograms.py is a mirror of its counterpart from
[mozilla-central](https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/parse_histograms.py).
To update it to the latest version you can run
```bash
bin/update_parser_scripts
```
Note that this file was formerly called histogram_tools.py and was renamed in Bug 1419761.

## Testing locally

To test/debug this package locally, the recommended procedure is to build a
docker image with the appropriate dependencies, then execute the unit
tests inside it:

```bash
docker build -t moztelemetry_docker .
./runtests.sh # will run tests inside docker container
```

You can also run a subset of the tests by passing arguments to `runtests.sh`:

```bash
./runtests.sh -ktest_unpack # runs only tests with key "test_unpack"
./runtests.sh tests/heka # runs only tests in tests/heka
```

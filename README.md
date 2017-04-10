 
# python_moztelemetry [![Build Status](https://travis-ci.org/mozilla/python_moztelemetry.svg?branch=master)](https://travis-ci.org/mozilla/python_moztelemetry) [![Coverage Status](https://coveralls.io/repos/github/mozilla/python_moztelemetry/badge.svg?branch=master)](https://coveralls.io/github/mozilla/python_moztelemetry?branch=master) [![Documentation Status](http://readthedocs.org/projects/python_moztelemetry/badge/?version=latest)](https://python_moztelemetry.readthedocs.io/?badge=latest)

Spark bindings for Mozilla Telemetry

## Installing from pypi
- To install this package from pypi run:
```
pip install python_moztelemetry
```

## Updating the package on PyPI
- Create / update your `~/.pypirc`
```
[distutils]
index-servers=pypi
[pypi]
repository = https://pypi.python.org/pypi
[pypi]
username:example_user
password:example_pass
```
- Fetch the latest code with `git pull`
- Update PyPI with `python setup.py sdist upload`


## Updating histogram_tools.py
moztelemetry/histogram_tools.py is a mirror of its counterpart from
[mozilla-central](https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/histogram_tools.py).
To update it to the latest version you can run
```bash
bin/update_histogram_tools
```

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
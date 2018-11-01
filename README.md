
# python_moztelemetry [![CircleCI Build Status](https://circleci.com/gh/mozilla/python_moztelemetry/tree/master.svg?style=svg)](https://circleci.com/gh/mozilla/python_moztelemetry/tree/master) [![Documentation Status](http://readthedocs.org/projects/python_moztelemetry/badge/?version=latest)](https://python_moztelemetry.readthedocs.io/?badge=latest) [![Updates](https://pyup.io/repos/github/mozilla/python_moztelemetry/shield.svg)](https://pyup.io/repos/github/mozilla/python_moztelemetry/) [![codecov.io](https://codecov.io/github/mozilla/python_moztelemetry/coverage.svg?branch=master)](https://codecov.io/github/mozilla/python_moztelemetry?branch=master)

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

## Updating message_pb2.py

`moztelemetry/heka/message_pb2.py` is generated from
[mozilla-services/heka](https://github.com/mozilla-services/heka/blob/dev/message/message.proto).
To regenerate it, you'll need to install a `protobuf` package for your system.
To avoid installing go-specific extensions, remove the `gogo.proto` import
and the `gogoproto` options and then run `protoc`:

```
git clone https://github.com/mozilla-services/heka
mkdir pythonfiles/
protoc -I heka/message --python_out pythonfiles/ heka/message/message.proto
```

## Testing locally

To test/debug this package locally, you can run exactly the job that
CircleCI runs for continuous integration by
[installing the CircleCI local CLI](https://circleci.com/docs/2.0/local-cli/#installing-the-circleci-local-cli-on-macos-and-linux-distros)
and invoking:

```bash
circleci build --job py36
```

See [.circleci/config.yml] for the other configured job names
(for running tests on different python versions).

The above process takes a few minutes to run every time, so there
is also a `bin/test` script that builds a docker image and
python environment (both of which are cached locally) and allows
you to run a subset of tests. Here are some sample invocations:

```bash
./bin/test tests/ -k test_unpack  # runs only tests with key "test_unpack"
./bin/test tests/heka/            # runs only tests in tests/heka
PYTHON_VERSION=2.7 ./bin/test     # specify a python version
```

It's also possible to run the tests locally outside of docker
by invoking `tox` directly, but the details of doing so depend
on your local development environment and are outside the scope
of these docs. Be aware that you will need to have a working
installation of Java and libsnappy, likely via your OS's package
manager (i.e. `brew install snappy` on MacOS).

If you're receiving mysterious errors, try removing cached files via:

    ./bin/clean

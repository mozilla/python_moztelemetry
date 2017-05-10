#!/bin/bash

# abort immediately on any failure
set -e

# if we are not inside the docker container, run this command *inside* the
# docker container
if [ ! -f /.dockerenv ]; then
    docker run -t -i -v $PWD:/python_moztelemetry moztelemetry_docker ./runtests.sh "$@"
    exit $?
fi

# Start hbase
/hbase-$HBASE_VERSION/bin/start-hbase.sh
/hbase-$HBASE_VERSION/bin/hbase-daemon.sh start thrift

# Run tests
if [ $# -gt 0 ]; then
    ARGS="$@"
    coverage run --source=moztelemetry setup.py test --addopts "${ARGS}"
else
    coverage run --source=moztelemetry setup.py test
fi

# Report coveralls output if using travis
if [ $TRAVIS_BRANCH ]; then
  coveralls
fi

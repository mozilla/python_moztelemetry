#!/bin/bash

# abort immediately on any failure
set -e

# if we are not inside the docker container, run this command *inside* the
# docker container
if [ ! -f /.dockerenv ]; then
    docker run -t -i -v $PWD:/python_moztelemetry moztelemetry_docker ./runtests.sh "$@"
    exit $?
fi

# Run tests
if [ $# -gt 0 ]; then
    ARGS="$@"
    python setup.py test --addopts "${ARGS}"
else
    python setup.py test
fi

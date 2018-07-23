#!/bin/bash

# abort immediately on any failure
set -e

PYTHON_VERSION="${PYTHON_VERSION:-2.7}"
REBUILD_DOCKER="${REBUILD_DOCKER:-false}"

# if we are not inside the docker container, run this command *inside* the
# docker container
if [ ! -f /.dockerenv ]; then
    if $REBUILD_DOCKER || ! docker images moztelemetry_docker:python-"$PYTHON_VERSION" | grep -q moztelemetry_docker; then
        docker build -t moztelemetry_docker:python-"$PYTHON_VERSION" --build-arg PYTHON_VERSION="$PYTHON_VERSION" .
    fi
    docker run -t -i -v "$PWD":/python_moztelemetry moztelemetry_docker:python-"$PYTHON_VERSION" ./runtests.sh "$@"
    exit $?
fi

# Run tests
if [ $# -gt 0 ]; then
    ARGS="$@"
    coverage run --source=moztelemetry setup.py test --addopts "${ARGS}"
else
    coverage run --source=moztelemetry setup.py test
fi
coverage xml

#!/bin/sh

# if we are not inside the docker container, run this command *inside* the
# docker container
if [ ! -f /.dockerenv ]; then
    docker run moztelemetry_docker ./runtests.sh
    exit $?
fi

# Start hbase
/hbase-$HBASE_VERSION/bin/start-hbase.sh
/hbase-$HBASE_VERSION/bin/hbase-daemon.sh start thrift

# Run tests
coverage run --source=moztelemetry setup.py test --addopts "-v --timeout=60"

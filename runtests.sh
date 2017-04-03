#!/bin/sh

# Start hbase
/hbase-$HBASE_VERSION/bin/start-hbase.sh
/hbase-$HBASE_VERSION/bin/hbase-daemon.sh start thrift

# Run tests
coverage run --source=moztelemetry setup.py test --addopts "-v --timeout=60"
coveralls

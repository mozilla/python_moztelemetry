# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
import logging

import boto3
import pytest
from moto import mock_s3
from concurrent import futures

import findspark
findspark.init()
import pyspark

from moztelemetry import store


@pytest.fixture
def my_mock_s3(request, monkeypatch):
    """The purpose of this fixture is to setUp/tearDown the moto library"""
    m = mock_s3()
    m.start()

    def fin():
        m.stop()
    request.addfinalizer(fin)


class DummyPoolExecutor(futures.Executor):

    def __init__(self, *args, **kwargs):
        pass

    def map(self, fn, *iterables, **kwargs):
        return map(fn, *iterables)


@pytest.fixture()
def dummy_bucket(my_mock_s3):
    bucket = boto3.resource('s3').Bucket('my-test-bucket')
    bucket.create()
    return bucket


@pytest.fixture(scope='session')
def spark_context(request):
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.ERROR)
    sc = pyspark.SparkContext(master="local[8]")

    finalizer = lambda : sc.stop()
    request.addfinalizer(finalizer)

    return sc


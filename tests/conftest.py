# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import os

import boto3
import pytest
from moto import mock_s3
from concurrent import futures
from pyspark.sql import SparkSession


@pytest.fixture
def my_mock_s3(request):
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
def dummy_pool_executor(monkeypatch):
    # there seems to be a bad interaction between process pool executor and
    # the moto library, workaround this in tests by mocking it out
    monkeypatch.setattr(futures, 'ProcessPoolExecutor', DummyPoolExecutor)


@pytest.fixture()
def dummy_bucket(my_mock_s3):
    bucket = boto3.resource('s3').Bucket('my-test-bucket')
    bucket.create()
    return bucket


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession
        .builder
        .master("local")
        .appName("python_moztelemetry_test")
        .getOrCreate()
    )

    logger = logging.getLogger("py4j")
    logger.setLevel(logging.ERROR)

    yield spark
    spark.stop()


@pytest.fixture
def spark_context(spark):
    return spark.sparkContext


@pytest.fixture(scope='session')
def data_dir():
    here = os.path.dirname(__file__)
    return os.path.join(here, 'data')

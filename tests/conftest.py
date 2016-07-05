# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
import boto3
import pytest
from moto import mock_s3


@pytest.fixture
def my_mock_s3(request):
    """The purpose of this fixture is to setUp/tearDown the moto library"""
    m = mock_s3()
    m.start()

    def fin():
        m.stop()
    request.addfinalizer(fin)


@pytest.fixture()
def dummy_bucket(my_mock_s3):
    bucket = boto3.resource('s3').Bucket('my-test-bucket')
    bucket.create()
    return bucket


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true",
        help="run slow tests")

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
import tempfile

import boto3
import pytest

from moztelemetry.store import S3Store, InMemoryStore


@pytest.mark.parametrize('store_class', [S3Store, InMemoryStore])
def test_empty_store(dummy_bucket, store_class):
    store = store_class(dummy_bucket.name)
    keys = store.list_keys('/')
    assert list(keys) == []

    folders = store.list_folders()
    assert list(folders) == []

    assert store.is_prefix_empty('/')


@pytest.mark.parametrize('store_class', [S3Store, InMemoryStore])
def test_upload_key(dummy_bucket, store_class):
    store = store_class(dummy_bucket.name)
    key = 'my-key'
    value = 'my-value'

    with tempfile.TemporaryFile() as f:
        f.write(value)
        f.seek(0)
        store.upload_file(f, '', key)

    assert store.get_key(key).read() == value


@pytest.mark.parametrize('store_class', [S3Store, InMemoryStore])
def test_list_keys(dummy_bucket, store_class):
    store = store_class(dummy_bucket.name)
    keys = ('dir1/key1', 'dir2/value2')
    values = ('value1', 'value2')

    for index, key in enumerate(keys):
        with tempfile.TemporaryFile() as f:
            f.write(values[index])
            f.seek(0)
            store.upload_file(f, '', key)

    for index, item in enumerate(sorted(store.list_keys('dir1'))):
        assert item['key'] == 'dir1/key1'


@pytest.mark.parametrize('store_class', [S3Store, InMemoryStore])
def test_list_folders(dummy_bucket, store_class):
    store = store_class(dummy_bucket.name)
    keys = ('dir1/subdir1/key1', 'dir2/another-dir/key2')
    values = ('value1', 'value2')

    for index, key in enumerate(keys):
        with tempfile.TemporaryFile() as f:
            f.write(values[index])
            f.seek(0)
            store.upload_file(f, '', key)

    expected = ('dir1/subdir1/',)
    for index, item in enumerate(sorted(store.list_folders(prefix='dir1/'))):
        assert item == expected[index]


@pytest.mark.parametrize('store_class', [S3Store, InMemoryStore])
def test_get_key(dummy_bucket, store_class):
    store = store_class(dummy_bucket.name)
    key = 'key1'
    value = 'value1'

    with tempfile.TemporaryFile() as f:
        f.write(value)
        f.seek(0)
        store.upload_file(f, '', key)
    assert store.get_key(key).read() == value



@pytest.mark.parametrize('store_class', [S3Store, InMemoryStore])
def test_get_non_existing_key(dummy_bucket, store_class):
    store = store_class(dummy_bucket.name)

    with pytest.raises(Exception) as exc_info:
        store.get_key('random-key')
    assert str(exc_info.value) == 'Error retrieving key "random-key" from S3'


@pytest.mark.parametrize('store_class', [S3Store, InMemoryStore])
def test_delete_key(dummy_bucket, store_class):
    store = store_class(dummy_bucket.name)
    key = 'key1'
    value = 'value1'

    with tempfile.TemporaryFile() as f:
        f.write(value)
        f.seek(0)
        store.upload_file(f, '', key)

    store.delete_key(key)
    bucket = boto3.resource('s3').Bucket(store.bucket_name)
    assert len(list(bucket.objects.all())) == 0


@pytest.mark.parametrize('store_class', [S3Store, InMemoryStore])
def test_delete_non_existing_key(dummy_bucket, store_class):
    store = store_class(dummy_bucket.name)
    store.delete_key('random-key-2')


@pytest.mark.parametrize('store_class', [S3Store, InMemoryStore])
def test_is_prefix_empty(dummy_bucket, store_class):
    store = store_class(dummy_bucket.name)
    key = 'dir1/key1'
    value = 'value1'

    with tempfile.TemporaryFile() as f:
        f.write(value)
        f.seek(0)
        store.upload_file(f, '', key)

    assert not store.is_prefix_empty('dir1/')

    assert store.is_prefix_empty('random-dir/')

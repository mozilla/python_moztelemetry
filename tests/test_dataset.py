# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
import tempfile

import pytest

from moztelemetry.dataset import Dataset, group_by_size
from moztelemetry.store import S3Store


def test_where():
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], 'prefix/', {})
    clause = lambda x: True
    new_dataset = dataset.where('dim1', clause)

    assert new_dataset.bucket == dataset.bucket
    assert new_dataset.schema == dataset.schema
    assert new_dataset.prefix == dataset.prefix

    assert new_dataset is not dataset

    assert new_dataset.clauses == {'dim1': clause}



def test_where_wrong_dimension():
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], 'prefix/', {})
    clause = lambda x: True

    with pytest.raises(Exception) as exc_info:
        new_dataset = dataset.where('dim3', clause)

    assert str(exc_info.value) == 'The dimension dim3 doesn\'t exist'


def test_where_dupe_dimension():
    clause = lambda x: True
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], 'prefix/',
                      {'dim1': clause})

    with pytest.raises(Exception) as exc_info:
        new_dataset = dataset.where('dim1', clause)

    assert str(exc_info.value) == 'There should be only one clause for dim1'


def test_scan_no_dimensions():
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], 'prefix/', {})
    folders = dataset._scan([], ['prefix/',])

    assert folders == ['prefix/',]


def test_scan_no_clause(dummy_bucket):
    store = S3Store(dummy_bucket.name)
    key = 'dir1/dir2/key1'
    value = 'value1'

    with tempfile.TemporaryFile() as f:
        f.write(value)
        f.seek(0)
        store.upload_file(f, '', key)

    dataset = Dataset(dummy_bucket.name, ['dim1', 'dim2'], '', {})
    folders = dataset._scan(['dim1', 'subdir'], [''])
    assert list(folders) == ['dir1/dir2/']


def test_scan_with_clause(dummy_bucket):
    store = S3Store(dummy_bucket.name)
    keys = ('dir1/subdir1/key1', 'dir2/another-dir/key2')
    values = ('value1', 'value2')

    for index, key in enumerate(keys):
        with tempfile.TemporaryFile() as f:
            f.write(values[index])
            f.seek(0)
            store.upload_file(f, '', key)

    dataset = Dataset(dummy_bucket.name, ['dim1', 'dim2'], '',
                      {'dim1': lambda x: x == 'dir1'})

    folders = dataset._scan(['dim1', 'dim2'], [''])
    assert list(folders) == ['dir1/subdir1/']


def test_scan_with_prefix(dummy_bucket):
    store = S3Store(dummy_bucket.name)
    keys = ('prefix1/dir1/subdir1/key1', 'prefix2/dir2/subdir2/key2')
    values = ('value1', 'value2')

    for index, key in enumerate(keys):
        with tempfile.TemporaryFile() as f:
            f.write(values[index])
            f.seek(0)
            store.upload_file(f, '', key)

    dataset = Dataset(dummy_bucket.name, ['dim1', 'dim2'], '',
                      {'dim2': lambda x: x == 'subdir1'})

    folders = dataset._scan(['dim1', 'dim2',], ['prefix1/',])
    assert list(folders) == ['prefix1/dir1/subdir1/']



def test_summaries(dummy_bucket):
    store = S3Store(dummy_bucket.name)
    keys = ('dir1/subdir1/key1', 'dir2/subdir2/key2')
    values = ('value1', 'value2')

    for index, key in enumerate(keys):
        with tempfile.TemporaryFile() as f:
            f.write(values[index])
            f.seek(0)
            store.upload_file(f, '', key)

    dataset = Dataset(dummy_bucket.name, ['dim1', 'dim2'], '', {})

    summaries = dataset.summaries()

    for index, summary in enumerate(summaries):
        assert summary.key == keys[index]


def test_summaries_with_limit(dummy_bucket):
    store = S3Store(dummy_bucket.name)
    keys = ('dir1/subdir1/key1', 'dir2/subdir2/key2')
    values = ('value1', 'value2')

    for index, key in enumerate(keys):
        with tempfile.TemporaryFile() as f:
            f.write(values[index])
            f.seek(0)
            store.upload_file(f, '', key)

    dataset = Dataset(dummy_bucket.name, ['dim1', 'dim2'], '', {})
    summaries = list(dataset.summaries(1))

    assert len(summaries) == 1

    assert summaries[0].key == keys[0]


class DummyS3Summary:
    def  __init__(self, size):
        self.size=size

def test_group_by_size():
    obj_list = [DummyS3Summary(2**29)] * 5

    groups = group_by_size(obj_list)

    grouped_sizes = [[obj.size for obj in obj_list] for obj_list in groups]

    assert grouped_sizes == [[2**29, 2**29, 2**29], [2**29, 2**29]]

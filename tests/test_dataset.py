# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
from concurrent import futures
import pytest

from moztelemetry.dataset import Dataset, group_by_size
from moztelemetry.store import InMemoryStore


def test_repr():
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], prefix='prefix/')
    assert "Dataset(bucket=test-bucket, schema=['dim1', 'dim2']," in repr(dataset)


def test_where():
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], prefix='prefix/')
    clause = lambda x: True
    new_dataset = dataset.where('dim1', clause)

    assert new_dataset is not dataset
    assert new_dataset.clauses == {'dim1': clause}


def test_where_wrong_dimension():
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], prefix='prefix/')
    clause = lambda x: True

    with pytest.raises(Exception) as exc_info:
        new_dataset = dataset.where('dim3', clause)

    assert str(exc_info.value) == 'The dimension dim3 doesn\'t exist'


def test_where_dupe_dimension():
    clause = lambda x: True
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], prefix='prefix/',
                      clauses={'dim1': clause})

    with pytest.raises(Exception) as exc_info:
        new_dataset = dataset.where('dim1', clause)

    assert str(exc_info.value) == 'There should be only one clause for dim1'


def test_scan_no_dimensions(dummy_executor):
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], prefix='prefix/')
    with futures.ProcessPoolExecutor(1) as executor:
        folders = dataset._scan([], ['prefix/',], {}, executor)

    assert folders == ['prefix/',]


def test_scan_no_clause(dummy_executor):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    key = 'dir1/dir2/key1'
    value = 'value1'
    store.store[key] = value

    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store)
    with futures.ProcessPoolExecutor(1) as executor:
        folders = dataset._scan(['dim1', 'subdir'], [''], {}, executor)
    assert list(folders) == ['dir1/dir2/']


def test_scan_with_clause(dummy_executor):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    store.store['dir1/subdir1/key1'] = 'value1'
    store.store['dir2/another-dir/key2'] = 'value2'

    dataset = Dataset(bucket_name, ['dim1', 'dim2'],
                      clauses={'dim1': lambda x: x == 'dir1'}, store=store)
    with futures.ProcessPoolExecutor(1) as executor:
        folders = dataset._scan(['dim1', 'dim2'], [''], dataset.clauses, executor)
    assert list(folders) == ['dir1/subdir1/']


def test_scan_with_prefix(dummy_executor):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    store.store['prefix1/dir1/subdir1/key1'] = 'value1'
    store.store['prefix2/dir2/another-dir/key2'] = 'value2'
    dataset = Dataset(bucket_name, ['dim1', 'dim2'],
                      clauses={'dim2': lambda x: x == 'subdir1'}, store=store)
    with futures.ProcessPoolExecutor(1) as executor:
        folders = dataset._scan(['dim1', 'dim2',], ['prefix1/',], {}, executor)
    assert list(folders) == ['prefix1/dir1/subdir1/']


def test_summaries(dummy_executor):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    store.store['dir1/subdir1/key1'] = 'value1'
    store.store['dir2/subdir2/key2'] = 'value2'

    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store)

    summaries = dataset._summaries()
    assert len(list(summaries)) == 2

    for item in summaries:
        assert item['key'] in store.store
        assert item['size'] == len(store.store[item['key']])


def test_summaries_with_limit(dummy_executor):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    store.store['dir1/subdir1/key1'] = 'value1'
    store.store['dir2/subdir2/key2'] = 'value2'
    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store)
    summaries = list(dataset._summaries(1))

    assert len(summaries) == 1

    assert summaries[0]['key'] in store.store


def test_group_by_size():
    obj_list = [dict(size=2**29)] * 5
    groups = group_by_size(obj_list)
    grouped_sizes = [[obj['size'] for obj in obj_list] for obj_list in groups]

    assert grouped_sizes == [[2**29, 2**29, 2**29], [2**29, 2**29]]


slow = pytest.mark.skipif(
    not pytest.config.getoption("--runslow"),
    reason="need --runslow option to run"
)

@slow
def test_records(spark_context, dummy_executor):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    store.store['dir1/subdir1/key1'] = 'value1'
    store.store['dir2/subdir2/key2'] = 'value2'
    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store)
    records = dataset.records(spark_context, decode=lambda x: x)
    records = records.collect()

    assert records == ['value1', 'value2']

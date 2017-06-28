# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
import json
import os

import boto3
from concurrent import futures
import pytest

import moztelemetry
from moztelemetry.dataset import Dataset, _group_by_size_greedy
from moztelemetry.store import InMemoryStore, S3Store


def test_repr():
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], prefix='prefix/')
    assert "Dataset(bucket=test-bucket, schema=['dim1', 'dim2']," in repr(dataset)


def test_where():
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], prefix='prefix/')

    def clause(x):
        return True
    new_dataset = dataset.where(dim1=clause)

    assert new_dataset is not dataset
    assert new_dataset.clauses == {'dim1': clause}


def test_select():
    dataset1 = Dataset('test-bucket', ['dim1', 'dim2']).select('field1', 'field2')
    dataset2 = Dataset('test-bucket', ['dim1', 'dim2']).select('field1', field2='field2')
    dataset3 = Dataset('test-bucket', ['dim1', 'dim2']).select(field1='field1', field2='field2')

    assert dataset1.selection == {
        'field1': 'field1',
        'field2': 'field2',
    }

    assert dataset1.selection == dataset2.selection == dataset3.selection

    dataset4 = Dataset('test-bucket', ['dim1', 'dim2']).select('field1', field2='f2', field3='f3')

    assert dataset4.selection == {
        'field1': 'field1',
        'field2': 'f2',
        'field3': 'f3',
    }

    dataset5 = dataset4.select('field4', field5='f5')

    assert dataset5.selection == {
        'field1': 'field1',
        'field2': 'f2',
        'field3': 'f3',
        'field4': 'field4',
        'field5': 'f5'
    }


def test_select_dupe_properties():
    dataset = Dataset('test-bucket', ['dim1', 'dim2']).select('field1')

    with pytest.raises(Exception) as exc_info:
        dataset.select('field1')

    assert str(exc_info.value) == 'The property field1 has already been selected'

    with pytest.raises(Exception) as exc_info:
        dataset.select(field1='keyword_field')

    assert str(exc_info.value) == 'The property field1 has already been selected'


def test_apply_selection():
    dataset = Dataset('test-bucket', ['dim1', 'dim2']).select('field1.field2')
    json_obj = {'field1': {'field2': 'value'}}

    assert dataset._apply_selection(json_obj) == {'field1.field2': 'value'}

    dataset = Dataset('test-bucket', ['dim1', 'dim2']).select(field='field1.field2')

    assert dataset._apply_selection(json_obj) == {'field': 'value'}

    dataset = Dataset('test-bucket', ['dim1', 'dim2']).select(field='foo.bar')

    assert dataset._apply_selection(json_obj) == {'field': None}


def test_where_exact_match():
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], prefix='prefix/')
    new_dataset = dataset.where(dim1='myvalue')
    assert new_dataset is not dataset
    assert new_dataset.clauses.keys() == ['dim1']
    condition = new_dataset.clauses['dim1']
    assert condition('myvalue')


def test_where_wrong_dimension():
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], prefix='prefix/')

    def clause(x):
        return True

    with pytest.raises(Exception) as exc_info:
        dataset.where(dim3=clause)

    assert str(exc_info.value) == 'The dimension dim3 doesn\'t exist'


def test_where_dupe_dimension():
    def clause(x):
        return True
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], prefix='prefix/',
                      clauses={'dim1': clause})

    with pytest.raises(Exception) as exc_info:
        dataset.where(dim1=clause)

    assert str(exc_info.value) == 'There should be only one clause for dim1'


def test_scan_no_dimensions():
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], prefix='prefix/')
    with futures.ProcessPoolExecutor(1) as executor:
        folders = dataset._scan([], ['prefix/', ], {}, executor)

    assert folders == ['prefix/', ]


def test_scan_no_clause():
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    key = 'dir1/dir2/key1'
    value = 'value1'
    store.store[key] = value

    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store)
    with futures.ProcessPoolExecutor(1) as executor:
        folders = dataset._scan(['dim1', 'subdir'], ['prefix'], {}, executor)
    assert list(folders) == ['prefix']


def test_scan_with_clause():
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    store.store['dir1/subdir1/key1'] = 'value1'
    store.store['dir2/another-dir/key2'] = 'value2'

    dataset = Dataset(bucket_name, ['dim1', 'dim2'],
                      clauses={'dim1': lambda x: x == 'dir1'}, store=store)
    with futures.ProcessPoolExecutor(1) as executor:
        folders = dataset._scan(['dim1', 'dim2'], [''], dataset.clauses, executor)
    assert list(folders) == ['dir1/']


def test_scan_multiple_where_params(spark_context):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    store.store['dir1/subdir1/key1'] = 'value1'
    store.store['dir1/another-dir/key2'] = 'value2'
    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store).where(dim1='dir1', dim2='subdir1')
    summaries = dataset.summaries(spark_context)
    expected_key = 'dir1/subdir1/key1'
    assert summaries == [{'key': expected_key, 'size': len(store.store[expected_key])}]


def test_scan_multiple_params():
    dataset = Dataset('test-bucket', ['dim1', 'dim2'], prefix='prefix/')
    new_dataset = dataset.where(dim1='myvalue')
    assert new_dataset is not dataset
    assert new_dataset.clauses.keys() == ['dim1']
    condition = new_dataset.clauses['dim1']
    assert condition('myvalue')


def test_summaries(spark_context):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    store.store['dir1/subdir1/key1'] = 'value1'
    store.store['dir2/subdir2/key2'] = 'value2'

    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store)

    summaries = dataset.summaries(spark_context)
    assert len(summaries) == 2

    for item in summaries:
        assert item['key'] in store.store
        assert item['size'] == len(store.store[item['key']])


def test_summaries_with_limit(spark_context):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    store.store['dir1/subdir1/key1'] = 'value1'
    store.store['dir2/subdir2/key2'] = 'value2'
    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store)
    summaries = dataset.summaries(spark_context, 1)

    assert len(summaries) == 1

    assert summaries[0]['key'] in store.store


def test_group_by_size_greedy():
    obj_list = [dict(size=i) for i in range(1, 5)]

    groups = _group_by_size_greedy(obj_list, 1)
    assert groups == [
        [{'size': 4}, {'size': 3}, {'size': 2}, {'size': 1}]
    ]
    groups = _group_by_size_greedy(obj_list, 2)
    assert groups == [
        [{'size': 4}, {'size': 2}],
        [{'size': 3}, {'size': 1}]
    ]
    groups = _group_by_size_greedy(obj_list, 3)
    assert groups == [
        [{'size': 4}, {'size': 1}],
        [{'size': 3}],
        [{'size': 2}]
    ]


@pytest.mark.slow
def test_records(spark_context):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    store.store['dir1/subdir1/key1'] = 'value1'
    store.store['dir2/subdir2/key2'] = 'value2'
    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store)
    records = dataset.records(spark_context, decode=lambda x: x)
    records = records.collect()

    assert records == ['value1', 'value2']


@pytest.mark.slow
def test_records_many_groups(spark_context, monkeypatch):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    for i in range(1, spark_context.defaultParallelism + 2):
        store.store['dir1/subdir1/key{}'.format(i)] = 'value{}'.format(i)
    # create one group per item
    monkeypatch.setattr(moztelemetry.dataset, '_group_by_size_greedy',
                        lambda x, _: [[y] for y in x])
    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store)
    records = dataset.records(spark_context, decode=lambda x: x)
    records = records.collect()

    assert records == ['value{}'.format(i) for i in range(1, spark_context.defaultParallelism + 2)]


@pytest.mark.slow
def test_records_sample(spark_context):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    for i in range(1, 100 + 1):
        key = 'dir{}/subdir{}/key{}'.format(*[i] * 3)
        value = 'value{}'.format(i)
        store.store[key] = value
    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store)

    records_1 = dataset.records(spark_context, decode=lambda x: x, sample=0.1, seed=None).collect()
    assert len(records_1) == 10

    records_2 = dataset.records(spark_context, decode=lambda x: x, sample=0.1, seed=None).collect()

    # The sampling seed is different, so we have two different samples.
    assert records_1 != records_2

    records_1 = dataset.records(spark_context, decode=lambda x: x, sample=0.1).collect()
    records_2 = dataset.records(spark_context, decode=lambda x: x, sample=0.1).collect()

    # Same seed, same sample.
    assert records_1 == records_2


@pytest.mark.slow
def test_records_summaries(spark_context):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    store.store['dir1/subdir1/key1'] = 'value1'
    store.store['dir2/subdir2/key2'] = 'value2'
    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store)
    records = dataset.records(spark_context, decode=lambda x: x,
                              summaries=[{'key': 'dir1/subdir1/key1', 'size': len('value1')}])
    records = records.collect()

    assert records == ['value1']


@pytest.mark.slow
def test_records_limit(spark_context):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    for i in range(1, 100 + 1):
        key = 'dir{}/subdir{}/key{}'.format(*[i] * 3)
        value = 'value{}'.format(i)
        store.store[key] = value
    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store)
    records = dataset.records(spark_context, decode=lambda x: x, limit=5)
    assert records.count() == 5


@pytest.mark.slow
def test_records_limit_and_sample(spark_context):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    for i in range(1, 100 + 1):
        key = 'dir{}/subdir{}/key{}'.format(*[i] * 3)
        value = 'value{}'.format(i)
        store.store[key] = value
    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store)
    records = dataset.records(spark_context, decode=lambda x: x, limit=5, sample=0.9)
    assert records.count() == 5


def decode(obj):
    value = obj.getvalue()
    return [json.loads(value)]


@pytest.mark.slow
def test_records_selection(spark_context):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    key = 'dir1/subdir1/key1'
    value = '{"a": {"b": { "c": "value"}}}'
    store.store[key] = value
    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store).select(field='a.b.c')
    records = dataset.records(spark_context, decode=decode)
    assert records.collect() == [{'field': 'value'}]

    # Check that concatenating `select`s works as expected
    records = dataset.select(field2='a.b').records(spark_context, decode=decode)
    assert records.collect() == [{'field': 'value', 'field2': {'c': 'value'}}]


@pytest.mark.slow
def test_records_print_output(spark_context, capsys):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    for i in range(1, 100+1):
        key = 'dir{}/subdir{}/key{}'.format(*[i]*3)
        value = 'value{}'.format(i)
        store.store[key] = value
    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store)
    dataset.records(spark_context, decode=lambda x: x)
    out, err = capsys.readouterr()
    assert out.rstrip() == "fetching 0.00066MB in 100 files..."


def test_dataset_from_source(my_mock_s3, monkeypatch):
    meta_bucket_name = 'net-mozaws-prod-us-west-2-pipeline-metadata'

    bucket = boto3.resource('s3').Bucket(meta_bucket_name)
    bucket.create()

    store = S3Store(meta_bucket_name)
    data_dir = os.path.join(os.path.dirname(__file__), 'data')

    with open(os.path.join(data_dir, 'sources.json')) as f:
        store.upload_file(f, '', 'sources.json')
    with open(os.path.join(data_dir, 'schema.json')) as f:
        store.upload_file(f, 'telemetry-2/', 'schema.json')
        f.seek(0)
        expected_dimensions = json.loads(f.read())['dimensions']

    dimensions = [dim['field_name'] for dim in expected_dimensions]

    assert Dataset.from_source('telemetry').schema == dimensions


def test_prefix_slash(spark_context):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    store.store['a/b/dir1/subdir1/key1'] = 'value1'
    store.store['a/b/dir2/subdir2/key2'] = 'value2'
    store.store['x/b/dir3/subdir3/key3'] = 'value3'
    store.store['a/c/dir4/subdir4/key4'] = 'value4'

    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store, prefix='a/b')

    summaries = dataset.summaries(spark_context)
    assert len(summaries) == 2

    for item in summaries:
        assert item['key'] in store.store
        assert item['size'] == len(store.store[item['key']])

    # be sure "where" still works
    summaries_filtered = dataset.where(dim1='dir1').summaries(spark_context)
    assert len(summaries_filtered) == 1
    assert summaries_filtered[0]['key'] == 'a/b/dir1/subdir1/key1'


def test_sanitized_dimensions(spark_context):
    bucket_name = 'test-bucket'
    store = InMemoryStore(bucket_name)
    store.store['dir_1/subdir1/key1'] = 'value1'
    store.store['dir_1/subdir2/key2'] = 'value2'
    store.store['dir_2/subdir3/key3'] = 'value3'
    store.store['dir_3/subdir4/key4'] = 'value4'

    dataset = Dataset(bucket_name, ['dim1', 'dim2'], store=store).where(dim1="dir-1")

    summaries = dataset.summaries(spark_context)
    assert len(summaries) == 2

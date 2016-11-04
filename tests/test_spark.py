# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
import json
import os
from string import Template
from uuid import uuid4

import pytest

from moztelemetry.store import InMemoryStore
from moztelemetry.dataset import Dataset
from moztelemetry.spark import get_pings


@pytest.fixture()
def test_store(monkeypatch):
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    with open(os.path.join(data_dir, 'schema.json')) as s:
        schema = json.loads(s.read())
    dimensions = [f['field_name'] for f in schema['dimensions']]
    dataset = Dataset('test-bucket', dimensions, InMemoryStore('test-bucket'))

    @staticmethod
    def from_source(source_name):
        return dataset

    monkeypatch.setattr(Dataset, 'from_source', from_source)

    return dataset.store


def upload_ping(store, value, **kwargs):
    """Upload value to a given store"""
    ping_key_template = Template('$submission_date/$source_name/'
                         '$source_version/$doc_type/$app/$channel/'
                         '$version/$build_id/$filename')
    dimensions = {
        'submission_date': '20160805',
        'source_name': 'telemetry',
        'source_version': '4',
        'doc_type': 'saved_session',
        'app': 'Firefox',
        'channel': 'nightly',
        'version': '51.0a1',
        'build_id': '20160801074053',
        'filename': uuid4()
    }
    dimensions.update(kwargs)
    key = ping_key_template.substitute(**dimensions)
    store.store[key] = value


@pytest.fixture
def mock_message_parser(monkeypatch):
    # monkeypatch the default `decoder` argument of `records`
    monkeypatch.setattr('moztelemetry.heka_message_parser.parse_heka_message',
                        lambda message: (message.getvalue(),))


test_data_for_exact_match = [
    ('doc_type', 'saved_session', 'main'),
    ('app', 'Firefox', 'Thunderbird'),
    ('version', '48.0', '46.0'),
    ('source_name', 'telemetry', 'other source'),
    ('source_version', '4', '2'),
]


@pytest.mark.slow
@pytest.mark.parametrize('filter_name,exact,wrong', test_data_for_exact_match)
def test_get_pings_by_exact_match(test_store, mock_message_parser, spark_context,
                                  filter_name, exact, wrong):
    upload_ping(test_store, 'value1', **{filter_name: exact})
    upload_ping(test_store, 'value2', **{filter_name: wrong})
    pings = get_pings(spark_context, **{filter_name: exact})

    assert pings.collect() == ['value1']


test_data_for_range_match = [
    ('submission_date', '20160110', '20150101', '20160101', '20160120'),
    ('build_id', '20160801074050', '20160801074055', '20160801074049', '20160801074052'),
]


@pytest.mark.slow
@pytest.mark.parametrize('filter_name,exact,wrong,start,end', test_data_for_range_match)
def test_get_pings_by_range(test_store, mock_message_parser, spark_context,
                            filter_name, exact, wrong, start, end):
    upload_ping(test_store, 'value1', **{filter_name: exact})
    upload_ping(test_store, 'value2', **{filter_name: wrong})
    pings = get_pings(spark_context, **{filter_name: exact})

    assert pings.collect() == ['value1']

    pings = get_pings(spark_context, **{filter_name: (start, end)})

    assert pings.collect() == ['value1']


@pytest.mark.slow
def test_get_pings_multiple_by_range(test_store, mock_message_parser, spark_context):
    upload_ping(test_store, 'value1', **{f[0]: f[1] for f in test_data_for_range_match})
    upload_ping(test_store, 'value2', **{f[0]: f[2] for f in test_data_for_range_match})
    pings = get_pings(spark_context, **{f[0]: f[1] for f in test_data_for_range_match})

    assert pings.collect() == ['value1']

    pings = get_pings(spark_context, **{f[0]: (f[3], f[4]) for f in test_data_for_range_match})

    assert pings.collect() == ['value1']


def test_get_pings_fraction(test_store, mock_message_parser, spark_context):
    for i in range(1, 10+1):
        upload_ping(test_store, 'value', build_id=str(i))

    pings = get_pings(spark_context)

    assert pings.count() == 10

    pings = get_pings(spark_context, fraction=0.1)

    assert pings.count() == 1


def test_get_pings_wrong_schema(test_store, mock_message_parser, spark_context):
    with pytest.raises(ValueError):
        pings = get_pings(spark_context, schema=1)


def test_get_pings_multiple_filters(test_store, mock_message_parser, spark_context):
    filters = dict(submission_date='20160101', channel='beta')
    upload_ping(test_store, 'value1', **filters)
    filters['app'] = 'Thunderbird'
    upload_ping(test_store, 'value2', **filters)
    pings = get_pings(spark_context, **filters)

    assert pings.collect() == ['value2']


def test_get_pings_none_filter(test_store, mock_message_parser, spark_context):
    upload_ping(test_store, 'value1', app='Firefox')
    upload_ping(test_store, 'value2', app='Thuderbird')
    pings = get_pings(spark_context, app=None)

    assert sorted(pings.collect()) == ['value1', 'value2']

    pings = get_pings(spark_context, app='*')

    assert sorted(pings.collect()) == ['value1', 'value2']

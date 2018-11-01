# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import datetime as DT, date
from six import iteritems
import os
import pytest
import moztelemetry.standards as std


def parse(date_string, format="%Y%m%d"):
    return DT.strptime(date_string, format)


def test_unix_time_nanos():
    d = parse("20170101")
    assert std.unix_time_nanos(d) == 1.4832288e+18


def test_daynum_to_date():
    assert std.daynum_to_date(0) == date(1970, 1, 1)
    assert std.daynum_to_date(5) == date(1970, 1, 6)
    assert std.daynum_to_date(5, max_days=4) == date(1970, 1, 5)
    assert std.daynum_to_date(None) is None
    assert std.daynum_to_date("five") is None


def test_snap_to_beginning_of_week():
    # Test default weekday_start
    tests = {
        "20170101": "20170101",
        "20170102": "20170101",
        "20170103": "20170101",
        "20170104": "20170101",
        "20170105": "20170101",
        "20170106": "20170101",
        "20170107": "20170101",
        "20170108": "20170108",
        "20170109": "20170108",
        "20170110": "20170108",
    }
    for arg, expected in iteritems(tests):
        d = parse(arg)
        e = parse(expected)
        assert e == std.snap_to_beginning_of_week(d)

    # Test a different weekday_start
    tests = {
        "20170101": "20161226",
        "20170102": "20170102",
        "20170103": "20170102",
        "20170104": "20170102",
        "20170105": "20170102",
        "20170106": "20170102",
        "20170107": "20170102",
        "20170108": "20170102",
        "20170109": "20170109",
        "20170110": "20170109",
    }
    for arg, expected in iteritems(tests):
        d = parse(arg)
        e = parse(expected)
        assert e == std.snap_to_beginning_of_week(d, weekday_start="Monday")


def test_snap_to_beginning_of_month():
    tests = {
        "20170101": "20170101",
        "20170102": "20170101",
        "20170325": "20170301",
        "20000101": "20000101",
        "20161231": "20161201",
    }
    for arg, expected in iteritems(tests):
        d = parse(arg)
        e = parse(expected)
        assert e == std.snap_to_beginning_of_month(d)


@pytest.fixture
def sample_data_path(data_dir):
    return os.path.join(data_dir, 'sample_main_summary', 'v1')


@pytest.fixture
def sample_data(spark, sample_data_path):
    data = spark.read.parquet(sample_data_path)
    return data


@pytest.fixture
def sample_data_merged(spark, sample_data_path):
    reader = spark.read.option("mergeSchema", "true")
    data = reader.parquet(sample_data_path)
    return data


def test_parquet_read(sample_data):
    count = sample_data.count()
    assert count == 90


def test_schema_evolution(sample_data, sample_data_merged):
    # submission_date_s3, sample_id, document_id, a
    assert len(sample_data.columns) == 4

    # submission_date_s3, sample_id, document_id, a, b, c
    assert len(sample_data_merged.columns) == 6


def test_read_main_summary(spark, sample_data_path,
                           sample_data, sample_data_merged):
    data = std.read_main_summary(spark, path=sample_data_path)
    assert data.count() == sample_data.count()
    assert data.count() == sample_data_merged.count()
    assert len(data.columns) == len(sample_data_merged.columns)

    data = std.read_main_summary(spark, path=sample_data_path, mergeSchema=False)
    assert len(data.columns) == len(sample_data.columns)


def test_base_path(spark, sample_data_path):
    parts = "submission_date_s3=20170101/sample_id=2"
    path = "{}/{}".format(sample_data_path, parts)
    data = spark.read.option("basePath", sample_data_path) \
                     .option("mergeSchema", True) \
                     .parquet(path)
    assert data.count() == 10
    assert len(data.columns) == 4

    parts = ["submission_date_s3=20170101/sample_id=2",
             "submission_date_s3=20170102/sample_id=2"]
    paths = ["{}/{}".format(sample_data_path, p) for p in parts]
    data = spark.read.option("basePath", sample_data_path) \
                     .option("mergeSchema", True) \
                     .parquet(*paths)
    assert data.count() == 20
    assert len(data.columns) == 5


def test_part_read_main_summary(spark, sample_data_path):
    data = std.read_main_summary(spark, path=sample_data_path,
                                 submission_date_s3=['20170102'],
                                 sample_id=[2, 3])
    assert data.count() == 20
    assert len(data.columns) == 5

    data = std.read_main_summary(spark, path=sample_data_path,
                                 submission_date_s3=['20170102', '20170103'],
                                 sample_id=[2])
    assert data.count() == 20
    assert len(data.columns) == 6

    data = std.read_main_summary(spark, path=sample_data_path, mergeSchema=False,
                                 submission_date_s3=['20170102', '20170103'],
                                 sample_id=[2])
    assert data.count() == 20
    assert len(data.columns) == 5

    data = std.read_main_summary(spark, path=sample_data_path,
                                 submission_date_s3=['20170101', '20170103'],
                                 sample_id=[2])
    assert data.count() == 20
    assert len(data.columns) == 6

    data = std.read_main_summary(spark, path=sample_data_path, mergeSchema=False,
                                 submission_date_s3=['20170101', '20170103'],
                                 sample_id=[2])
    assert data.count() == 20
    assert len(data.columns) == 4

    data = std.read_main_summary(spark, path=sample_data_path,
                                 submission_date_s3=['20170101', '20170103'])
    assert data.count() == 60
    assert len(data.columns) == 6

    data = std.read_main_summary(spark, path=sample_data_path, sample_id=[1])
    assert data.count() == 30


def get_min_max_id(df):
    ids = df.select("document_id").orderBy("document_id").collect()
    return [ids[0], ids[-1]]


# A trailing slash shouldn't make a difference.
def test_trailing_slash(spark, sample_data_path):
    with_slash = sample_data_path + '/'
    without_slash = sample_data_path

    assert with_slash[-1] == '/'
    assert without_slash[-1] != '/'

    dw = std.read_main_summary(spark, path=with_slash, submission_date_s3=['20170102'])
    dwo = std.read_main_summary(spark, path=without_slash, submission_date_s3=['20170102'])

    dw_min, dw_max = get_min_max_id(dw)
    dwo_min, dwo_max = get_min_max_id(dwo)

    assert dw_min == dwo_min
    assert dw_max == dwo_max

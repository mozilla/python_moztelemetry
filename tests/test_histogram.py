import pytest
import pandas as pd
from moztelemetry.histogram import Histogram, CATEGORICAL_HISTOGRAM_SPILL_BUCKET_NAME

def setup_module():
    global categorical_hist, series
    categorical_hist = Histogram("TELEMETRY_TEST_CATEGORICAL", [2, 1, 0, 0])
    series = pd.Series([2, 1, 0, 0], index=['CommonLabel', 'Label2', 'Label3', CATEGORICAL_HISTOGRAM_SPILL_BUCKET_NAME], dtype='int64')

@pytest.mark.slow
def test_histogram_with_computed_value():
    # Histogram with computed value
    Histogram("GC_REASON_2",
              [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 1,
               0, 0, 0, 11, 36, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2517, -1, -1,
               116979, 0])


@pytest.mark.slow
def test_histogram_without_revision():
    # Histogram without revision
    Histogram("STARTUP_CRASH_DETECTED",
              [1, 0, 0, 0, -1, -1, 0, 0],
              "http://hg.mozilla.org/mozilla-central/rev/da2f28836843")


@pytest.mark.slow
def test_histogram_with_revision():
    # Histogram with revision
    Histogram("HTTPCONNMGR_USED_SPECULATIVE_CONN",
              [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 1, 0.693147182464599, 0.480453014373779, -1,
               -1],
              "http://hg.mozilla.org/mozilla-central/rev/37ddc5e2eb72")


@pytest.mark.slow
def test_startup_histogram():
    # Startup histogram
    Histogram("STARTUP_HTTPCONNMGR_USED_SPECULATIVE_CONN",
              [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 1, 0.693147182464599, 0.480453014373779,
               -1, -1])


def test_categorical_histogram():
    assert all(categorical_hist.buckets == series)


def test_cateogrical_histogram_median():
    with pytest.raises(AssertionError):
        categorical_hist.get_value(only_median=True)


def test_categorical_histogram_value():
    assert all(categorical_hist.get_value() == series)


def test_categorical_histogram_percentile():
    with pytest.raises(AssertionError):
        categorical_hist.percentile(50)


def test_categorical_histogram_add():
    cat2 = Histogram("TELEMETRY_TEST_CATEGORICAL", [1, 1, 0, 1])
    added = categorical_hist + cat2
    assert added.buckets['CommonLabel'] == 3
    assert added.buckets['Label2'] == 2
    assert added.buckets['Label3'] == 0
    assert added.buckets[CATEGORICAL_HISTOGRAM_SPILL_BUCKET_NAME] == 1

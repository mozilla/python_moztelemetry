import pytest

from moztelemetry.histogram import Histogram


slow = pytest.mark.skipif(
    not pytest.config.getoption("--runslow"),
    reason="need --runslow option to run"
)


@slow
def test_histogram_with_computed_value():
    # Histogram with computed value
    Histogram("GC_REASON_2",
              [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 1,
               0, 0, 0, 11, 36, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2517, -1, -1,
               116979, 0])


@slow
def test_histogram_without_revision():
    # Histogram without revision
    Histogram("STARTUP_CRASH_DETECTED",
              [1, 0, 0, 0, -1, -1, 0, 0],
              "http://hg.mozilla.org/mozilla-central/rev/da2f28836843")


@slow
def test_histogram_with_revision():
    # Histogram with revision
    Histogram("HTTPCONNMGR_USED_SPECULATIVE_CONN",
              [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 1, 0.693147182464599, 0.480453014373779, -1,
               -1],
              "http://hg.mozilla.org/mozilla-central/rev/37ddc5e2eb72")


@slow
def test_startup_histogram():
    # Startup histogram
    Histogram("STARTUP_HTTPCONNMGR_USED_SPECULATIVE_CONN",
              [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 1, 0.693147182464599, 0.480453014373779,
               -1, -1])

#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This module implements test coverage for the stats functions in stats.py.
"""
import itertools

import numpy.random
import scipy.stats

import pytest
from moztelemetry import stats


def l2d(values):
    # Convert a list of values to a histogram representation.
    d = {}
    for v in values:
        d[v] = d.get(v, 0) + 1
    return d


# A normally distributed sample set.
norm1 = list(numpy.random.normal(5, 3.25, 1000))
norm2 = list(numpy.random.normal(6, 2.5, 1000))

# A uniformly distributed sample set.
uni1 = numpy.random.randint(1, 100, 1000)
uni2 = numpy.random.randint(10, 120, 900)

# A skewed normal distribution.
skew1 = list(scipy.stats.skewnorm.rvs(10, size=1000))
skew2 = list(scipy.stats.skewnorm.rvs(5, size=900))


samples = {
    'normalized': (norm1, norm2),
    'uniform': (uni1, uni2),
    'skewed': (skew1, skew2),
}


def test_rank():
    assert stats._rank({1: 1}) == {1: 1.0}
    assert stats._rank({1: 5, 2: 4, 3: 3, 4: 2, 5: 1}) == {
        1: 3.0,
        2: 7.5,
        3: 11.0,
        4: 13.5,
        5: 15.0,
    }


def test_tie_correct():
    assert stats._tie_correct({}) == 1.0
    assert stats._tie_correct({1: 1}) == 1.0
    assert stats._tie_correct({1: 48}) == 0.0


def test_ndtr():
    # Test invalid values raise an error.
    with pytest.raises(TypeError):
        stats.ndtr(None)
    with pytest.raises(ValueError):
        stats.ndtr('a')

    assert round(stats.ndtr(0), 6) == 0.5
    assert round(stats.ndtr(1), 6) == 0.841345
    assert round(stats.ndtr(2), 6) == 0.977250
    assert round(stats.ndtr(3), 6) == 0.998650

    assert round(stats.ndtr(0), 6) == round(scipy.special.ndtr(0), 6)
    assert round(stats.ndtr(1), 6) == round(scipy.special.ndtr(1), 6)
    assert round(stats.ndtr(1.5), 6) == round(scipy.special.ndtr(1.5), 6)
    assert round(stats.ndtr(2), 6) == round(scipy.special.ndtr(2), 6)
    assert round(stats.ndtr(3), 6) == round(scipy.special.ndtr(3), 6)


def test_mann_whitney_u():
    # Test case of all values being the same, which results in a
    # zero value tie correction.
    with pytest.raises(ValueError):
        stats.mann_whitney_u({0: 0, 1: 48}, {0: 0, 1: 40})

    # Test different distributions against each other, including
    # like distributions against themselves.
    distribution_types = ('normalized', 'uniform', 'skewed')
    for sample1, sample2 in itertools.product(distribution_types, repeat=2):

        arr1, arr2 = samples[sample1][0], samples[sample2][1]
        hist1, hist2 = l2d(arr1), l2d(arr2)

        # Basic test, with defaults.
        res = stats.mann_whitney_u(hist1, hist2)
        sci = scipy.stats.mannwhitneyu(arr1, arr2)
        assert res.u == sci.statistic
        assert round(res.p, 6) == round(sci.pvalue, 6)

        # Test that order of samples doesn't matter.
        res = stats.mann_whitney_u(hist2, hist1)
        sci = scipy.stats.mannwhitneyu(arr1, arr2)
        assert res.u == sci.statistic
        assert round(res.p, 6) == round(sci.pvalue, 6)

        # Test exact same samples.
        res = stats.mann_whitney_u(hist1, hist1)
        sci = scipy.stats.mannwhitneyu(arr1, arr1)
        assert res.u == sci.statistic
        assert round(res.p, 6) == round(sci.pvalue, 6)

        # Test with use_continuity = False.
        res = stats.mann_whitney_u(hist1, hist2, False)
        sci = scipy.stats.mannwhitneyu(arr1, arr2, False)
        assert res.u == sci.statistic
        assert round(res.p, 6) == round(sci.pvalue, 6)

#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import division

import math
from collections import namedtuple


def _rank(sample):
    """
    Assign numeric ranks to all values in the sample.

    The ranks begin with 1 for the smallest value. When there are groups of
    tied values, assign a rank equal to the midpoint of unadjusted rankings.

    E.g.::

        >>> rank({3: 1, 5: 4, 9: 1})
        {3: 1.0, 5: 3.5, 9: 6.0}

    """
    rank = 1
    ranks = {}

    for k in sorted(sample.keys()):
        n = sample[k]
        ranks[k] = rank + (n - 1) / 2
        rank += n

    return ranks


def _tie_correct(sample):
    """
    Returns the tie correction value for U.

    See: https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.tiecorrect.html

    """
    tc = 0
    n = sum(sample.values())

    if n < 2:
        return 1.0  # Avoid a ``ZeroDivisionError``.

    for k in sorted(sample.keys()):
        tc += math.pow(sample[k], 3) - sample[k]
    tc = 1 - tc / (math.pow(n, 3) - n)

    return tc


def ndtr(a):
    """
    Returns the area under the Gaussian probability density function,
    integrated from minus infinity to x.

    See: https://docs.scipy.org/doc/scipy/reference/generated/scipy.special.ndtr.html#scipy.special.ndtr

    """
    sqrth = math.sqrt(2) / 2
    x = float(a) * sqrth
    z = abs(x)
    if z < sqrth:
        y = 0.5 + 0.5 * math.erf(x)
    else:
        y = 0.5 * math.erfc(z)
        if x > 0:
            y = 1 - y
    return y


mwu_result = namedtuple('Mann_Whitney_U', ('u', 'p'))


def mann_whitney_u(sample1, sample2, use_continuity=True):
    """
    Computes the Mann-Whitney rank test on both samples.

    Each sample is expected to be of the form::

        {1: 5, 2: 20, 3: 12, ...}

    Returns a named tuple with:
        ``u`` equal to min(U for sample1, U for sample2), and
        ``p`` equal to the p-value.

    """
    # Merge dictionaries, adding values if keys match.
    sample = sample1.copy()
    for k, v in sample2.items():
        sample[k] = sample.get(k, 0) + v

    # Create a ranking dictionary using same keys for lookups.
    ranks = _rank(sample)

    sum_of_ranks = sum([sample1[k] * ranks[k] for k, v in sample1.items()])
    n1 = sum(sample1.values())
    n2 = sum(sample2.values())

    # Calculate Mann-Whitney U for both samples.
    u1 = sum_of_ranks - (n1 * (n1 + 1)) / 2
    u2 = n1 * n2 - u1

    tie_correction = _tie_correct(sample)
    if tie_correction == 0:
        raise ValueError('All provided sample values are identical.')

    sd_u = math.sqrt(tie_correction * n1 * n2 * (n1 + n2 + 1) / 12.0)
    mean_rank = n1 * n2 / 2.0 + 0.5 * use_continuity
    z = abs((max(u1, u2) - mean_rank) / sd_u)

    return mwu_result(min(u1, u2), ndtr(-z))

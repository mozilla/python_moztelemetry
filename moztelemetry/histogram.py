#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import division

import requests
import histogram_tools
import re
import pandas as pd
import numpy as np
import ujson as json

from functools32 import lru_cache
from expiringdict import ExpiringDict

# Ugly hack to speed-up aggregation.
exponential_buckets = histogram_tools.exponential_buckets
linear_buckets = histogram_tools.linear_buckets
definition_cache = ExpiringDict(max_len=2**10, max_age_seconds=3600)

@lru_cache(maxsize=2**14)
def cached_exponential_buckets(*args, **kwargs):
    return exponential_buckets(*args, **kwargs)

@lru_cache(maxsize=2**14)
def cached_linear_buckets(*args, **kwargs):
    return linear_buckets(*args, **kwargs)

histogram_tools.exponential_buckets = cached_exponential_buckets
histogram_tools.linear_buckets = cached_linear_buckets

def _fetch_histograms_definition(revision):
    cached = definition_cache.get(revision, None)
    if cached is None:
        uri = (revision + "/toolkit/components/telemetry/Histograms.json").replace("rev", "raw-file")
        definition = requests.get(uri).content

        # see bug 920169
        definition = definition.replace('"JS::gcreason::NUM_TELEMETRY_REASONS"', "101")
        definition = definition.replace('"mozilla::StartupTimeline::MAX_EVENT_ID"', "12")
        definition = definition.replace('"80 + 1"', "81")

        parsed = json.loads(definition)
        definition_cache[revision] = parsed
        return parsed
    else:
        return cached

@lru_cache(maxsize=2**20)  # A LFU cache would be more appropriate.
def _get_cached_ranges(definition):
    return definition.ranges()

class Histogram:
    """ A class representing a histogram. """

    def __init__(self, name, instance, revision="http://hg.mozilla.org/mozilla-central/rev/tip"):
        """ Initialize a histogram from its name and a telemetry submission. """

        histograms_definition = _fetch_histograms_definition(revision)

        # TODO: implement centralized revision service which handles all the quirks...
        if name.startswith("USE_COUNTER_") or name.startswith("USE_COUNTER2_"):
            self.definition = histogram_tools.Histogram(name, {"kind": "boolean", "description": "", "expires_in_version": "never"})
        else:
            try:
                self.definition = histogram_tools.Histogram(name, histograms_definition[name])
            except KeyError:
                self.definition = histogram_tools.Histogram(name, histograms_definition[re.sub("^STARTUP_", "", name)])

        self.kind = self.definition.kind()
        self.name = name

        if isinstance(instance, list) or isinstance(instance, np.ndarray) or isinstance(instance, pd.Series):
            if len(instance) == self.definition.n_buckets():
                values = instance
            else:
                values = instance[:-5]
            self.buckets = pd.Series(values, index=self.definition.ranges(), dtype='int64')
        else:
            entries = {int(k): v for k, v in instance["values"].items()}
            self.buckets = pd.Series(entries, index=self.definition.ranges(), dtype='int64').fillna(0)

    def __str__(self):
        """ Returns a string representation of the histogram. """
        return str(self.buckets)

    def get_value(self, only_median=False, autocast=True):
        """
        Returns a scalar for flag and count histograms. Otherwise it returns either the
        raw histogram represented as a pandas Series or just the median if only_median
        is True.
        If autocast is disabled the underlying pandas series is always returned as is.
        """

        if not autocast:
            return self.buckets

        if self.kind in ["exponential", "linear", "enumerated", "boolean"]:
            return float(self.percentile(50)) if only_median else self.buckets
        elif self.kind == "count":
            return long(self.buckets[0])
        elif self.kind == "flag":
            return self.buckets[1] == 1
        else:
            assert(False) # Unsupported histogram kind

    def get_definition(self):
        """ Returns the definition of the histogram. """
        return self.definition

    def percentile(self, percentile):
        """ Returns the nth percentile of the histogram. """
        assert(percentile >= 0 and percentile <= 100)
        assert(self.kind in ["exponential", "linear", "enumerated", "boolean"])

        fraction = percentile/100
        to_count = fraction*self.buckets.sum()
        percentile_bucket = 0

        for percentile_bucket in range(len(self.buckets)):
            freq = self.buckets.values[percentile_bucket]
            if to_count - freq <= 0:
                break
            to_count -= freq

        percentile_lower_boundary = self.buckets.index[percentile_bucket]
        percentile_frequency = self.buckets.values[percentile_bucket]

        if percentile_bucket == len(self.buckets) - 1 or percentile_frequency == 0:
            return percentile_lower_boundary

        width = self.buckets.index[percentile_bucket + 1] - self.buckets.index[percentile_bucket]
        return percentile_lower_boundary + width*to_count/percentile_frequency

    def __add__(self, other):
        return Histogram(self.name, self.buckets + other.buckets)


if __name__ == "__main__":
    # Histogram with computed value
    Histogram("GC_REASON_2", [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 11, 36, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2517, -1, -1, 116979, 0])

    # Histogram without revision
    Histogram("STARTUP_CRASH_DETECTED", [1, 0, 0, 0, -1, -1, 0, 0], "http://hg.mozilla.org/mozilla-central/rev/da2f28836843")

    # Histogram with revision
    Histogram("HTTPCONNMGR_USED_SPECULATIVE_CONN", [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0.693147182464599, 0.480453014373779, -1, -1], "http://hg.mozilla.org/mozilla-central/rev/37ddc5e2eb72")

    # Startup histogram
    Histogram("STARTUP_HTTPCONNMGR_USED_SPECULATIVE_CONN", [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0.693147182464599, 0.480453014373779, -1, -1])

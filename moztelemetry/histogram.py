from __future__ import division

import requests
import histogram_tools
import pandas as pd
import numpy as np

class Histogram:
    """ A class representing a histogram. """

    _definitions = requests.get("https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/Histograms.json").json()

    def __init__(self, name, instance):
        """ Initialize a histogram from its name and a telemetry submission. """

        self.definition = histogram_tools.Histogram(name, Histogram._definitions[name])
        self.kind = self.definition.kind()

        if isinstance(instance, list) or isinstance(instance, np.ndarray):
            if len(instance) == self.definition.n_buckets():
                values = instance
            else:
                values = instance[:-5]
            self.buckets = pd.Series(values, index=self.definition.ranges())
        else:
            entries = {int(k): v for k, v in instance["values"].items()}
            self.buckets = pd.Series(entries, index=self.definition.ranges()).fillna(0)

    def __str__(self):
        """ Returns a string representation of the histogram. """
        return str(self.buckets)

    def get_value(self, only_median=False):
        """
        Returns a scalar for flag and count histograms. Otherwise it returns either the
        raw histogram represented as a pandas Series or just the median if only_median
        is True.
        """

        if self.kind in ["exponential", "linear", "enumerated", "boolean"]:
            return self.percentile(50) if only_median else self.buckets
        elif self.kind == "count":
            return self.buckets[0]
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

        if percentile_bucket == len(self.buckets) - 1:
            return float('nan')

        percentile_frequency = self.buckets.values[percentile_bucket]
        percentile_lower_boundary = self.buckets.index[percentile_bucket]
        width = self.buckets.index[percentile_bucket + 1] - self.buckets.index[percentile_bucket]
        return percentile_lower_boundary + width*to_count/percentile_frequency

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import division, print_function
import copy_reg
import functools
import json
import random
import re
import types
from copy import copy
from inspect import isfunction
from itertools import chain
from multiprocessing import cpu_count

import jmespath
from concurrent import futures
from .heka import message_parser

from .store import S3Store

MAX_CONCURRENCY = int(cpu_count() * 1.5)
SANITIZE_PATTERN = re.compile("[^a-zA-Z0-9_.]")


def _group_by_size_greedy(obj_list, tot_groups):
    """Partition a list of objects in even buckets

    The idea is to choose the bucket for an object in a round-robin fashion.
    The list of objects is sorted to also try to keep the total size in bytes
    as balanced as possible.
    :param obj_list: a list of dict-like objects with a 'size' property
    :param tot_groups: number of partitions to split the data into.
    :return: a list of lists, one for each partition.
    """
    sorted_list = sorted(obj_list, key=lambda x: x['size'], reverse=True)
    groups = [[] for _ in range(tot_groups)]
    for index, obj in enumerate(sorted_list):
        current_group = groups[index % len(groups)]
        current_group.append(obj)
    return groups


def _pickle_method(m):
    """Make instance methods pickable

    See http://stackoverflow.com/questions/1816958/cant-pickle-type-instancemethod-when-using-pythons-multiprocessing-pool-ma/1816969#1816969
    """
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


copy_reg.pickle(types.MethodType, _pickle_method)


class Dataset:
    """Represents a collection of objects on S3.

    A Dataset can have zero, one or many filters, which are refined using the
    `where` method. The result of refining a Dataset is a Dataset itself, so
    it's possible to chain multiple `where` clauses together.

    The actual data retrieval is triggered by the `records` method, which returns
    a Spark RDD containing the list of records retrieved. To call `records`
    a SparkContext object must be provided.

    Usage example::

        bucket = 'test-bucket'
        schema = ['submissionDate', 'docType', 'platform']

        records = Dataset(bucket, schema) \\
            .select(
                'clientId',
                os_name='environment.system.os.name',
                first_paint='payload.simpleMeasurements.firstPaint',
                // Take the first 2 stacks for each thread hang.
                stack_list='payload.threadHangStats[].hangs[].stack[0:2]'
            ).where(
                docType='main',
                appUpdateChannel='nightly',
                submissionDate=lambda x: x.startswith('201607'),
            ).records(sc)

    For convenience Dataset objects can be created using the factory method
    `from_source`, that takes a source name (e.g. 'telemetry') and returns a
    new Dataset instance. The instance created will we aware of the list of
    dimensions, available on its `schema` attribute for inspection.
    """

    def __init__(self, bucket, schema, store=None,
                 prefix=None, clauses=None, selection=None):
        """Initialize a Dataset provided bucket and schema

        :param bucket: bucket name
        :param schema: a list of fields describing the structure of the dataset
        :param store: an instance of S3Store, potentially reused among several
        datasets
        :param prefix: a prefix to the
        :param clauses: mapping of fields -> callables to refine the dataset
        """
        self.bucket = bucket
        self.schema = schema
        self.prefix = prefix or ''
        self.clauses = clauses or {}
        self.store = store or S3Store(self.bucket)
        self.selection = selection or {}
        self.selection_compiled = {}

    def __repr__(self):
        return ('Dataset(bucket=%s, schema=%s, store=%s, prefix=%s, clauses=%s)'
                ) % (self.bucket, self.schema, self.store, self.prefix, self.clauses)

    def select(self, *properties, **aliased_properties):
        """Specify which properties of the dataset must be returned

        Property extraction is based on `JMESPath <http://jmespath.org>`_ expressions.
        This method returns a new Dataset narrowed down by the given selection.

        :param properties: JMESPath to use for the property extraction.
                           The JMESPath string will be used as a key in the output dictionary.
        :param aliased_properties: Same as properties, but the output dictionary will contain
                                   the parameter name instead of the JMESPath string.
        """
        if not (properties or aliased_properties):
            return self
        properties_pairs = zip(properties, properties)
        merged_properties = dict(properties_pairs + aliased_properties.items())

        for prop_name in (merged_properties.keys()):
            if prop_name in self.selection:
                raise Exception('The property {} has already been selected'.format(prop_name))

        new_selection = dict(self.selection.items() + merged_properties.items())

        return Dataset(self.bucket, self.schema, store=self.store,
                       prefix=self.prefix, selection=new_selection)

    def _compile_selection(self):
        if not self.selection_compiled:
            self.selection_compiled = dict((name, jmespath.compile(path))
                                           for name, path in self.selection.items())

    def _apply_selection(self, json_obj):
        if not self.selection:
            return json_obj

        # This is mainly for testing purposes.
        # For perfomance reasons the selection should be compiled
        # outside of this function.
        if not self.selection_compiled:
            self._compile_selection()

        return dict((name, path.search(json_obj))
                    for name, path in self.selection_compiled.items())

    def _sanitize_dimension(self, v):
        """Sanitize the given string by replacing illegal characters
        with underscores.

        For String conditions, we should pre-sanitize so that users of
        the `where` function do not need to know about the nuances of how
        S3 dimensions are sanitized during ingestion.

        See https://github.com/mozilla-services/lua_sandbox_extensions/blob/master/moz_telemetry/io_modules/moz_telemetry/s3.lua#L167

        :param v: a string value that should be sanitized.
        """
        return re.sub(SANITIZE_PATTERN, "_", v)

    def where(self, **kwargs):
        """Return a new Dataset refined using the given condition

        :param kwargs: a map of `dimension` => `condition` to filter the elements
            of the dataset. `condition` can either be an exact value or a
            callable returning a boolean value. If `condition` is a value, it is
            converted to a string, then sanitized.
        """
        clauses = copy(self.clauses)
        for dimension, condition in kwargs.items():
            if dimension in self.clauses:
                raise Exception('There should be only one clause for {}'.format(dimension))
            if dimension not in self.schema:
                raise Exception('The dimension {} doesn\'t exist'.format(dimension))
            if isfunction(condition) or isinstance(condition, functools.partial):
                clauses[dimension] = condition
            else:
                clauses[dimension] = functools.partial((lambda x, y: x == y), self._sanitize_dimension(str(condition)))
        return Dataset(self.bucket, self.schema, store=self.store,
                       prefix=self.prefix, clauses=clauses, selection=self.selection)

    def _scan(self, dimensions, prefixes, clauses, executor):
        if not dimensions or not clauses:
            return prefixes
        else:
            dimension = dimensions[0]
            clause = clauses.get(dimension)
            matched = executor.map(self.store.list_folders, prefixes)
            # Using chain to flatten the results of map
            matched = chain(*matched)
            if clause:
                matched = [x for x in matched if clause(x.strip('/').split('/')[-1])]
                del clauses[dimension]

            return self._scan(dimensions[1:], matched, clauses, executor)

    def summaries(self, sc, limit=None):
        """Summary of the files contained in the current dataset

        Every item in the summary is a dict containing a key name and the corresponding size of
        the key item in bytes, e.g.::
        {'key': 'full/path/to/my/key', 'size': 200}

        :param limit: Max number of objects to retrieve
        :return: An iterable of summaries
        """
        clauses = copy(self.clauses)
        schema = self.schema

        if self.prefix:
            schema = ['prefix'] + schema
            # Add a clause for the prefix that always returns True, in case
            # the output is not filtered at all (so that we do a scan/filter
            # on the prefix directory)
            clauses['prefix'] = lambda x: True

        with futures.ProcessPoolExecutor(MAX_CONCURRENCY) as executor:
            scanned = self._scan(schema, [self.prefix], clauses, executor)
        keys = sc.parallelize(scanned).flatMap(self.store.list_keys)
        return keys.take(limit) if limit else keys.collect()

    def records(self, sc, limit=None, sample=1, seed=42, decode=None, summaries=None):
        """Retrieve the elements of a Dataset

        :param sc: a SparkContext object
        :param limit: maximum number of objects to retrieve
        :param decode: an optional transformation to apply to the objects retrieved
        :param sample: percentage of results to return. Useful to return a sample
            of the dataset. This parameter is ignored when `limit` is set.
        :param seed: initialize internal state of the random number generator (42 by default).
            This is used to make the dataset sampling reproducible. It can be set to None to obtain
            different samples.
        :param summaries: an iterable containing a summary for each item in the dataset. If None,
            it will computed calling the summaries dataset.
        :return: a Spark rdd containing the elements retrieved

        """
        summaries = summaries or self.summaries(sc, limit)

        # Calculate the sample if summaries is not empty and limit is not set
        if summaries and limit is None and sample != 1:
            if sample < 0 or sample > 1:
                raise ValueError('sample must be between 0 and 1')
            # We want this sample to be reproducible.
            # See https://bugzilla.mozilla.org/show_bug.cgi?id=1318681
            seed_state = random.getstate()
            try:
                random.seed(seed)
                summaries = random.sample(summaries,
                                          int(len(summaries) * sample))
            finally:
                random.setstate(seed_state)

        # Obtain size in MB
        total_size = reduce(lambda acc, item: acc + item['size'], summaries, 0)
        total_size_mb = total_size / float(1 << 20)
        print("fetching %.5fMB in %s files..." % (total_size_mb, len(summaries)))

        groups = _group_by_size_greedy(summaries, 10 * sc.defaultParallelism)
        rdd = sc.parallelize(groups, len(groups)).flatMap(lambda x: x)

        if decode is None:
            decode = message_parser.parse_heka_message

        self._compile_selection()

        return rdd.map(lambda x: self.store.get_key(x['key'])) \
                  .flatMap(lambda x: decode(x)) \
                  .map(self._apply_selection)

    @staticmethod
    def from_source(source_name):
        """Create a Dataset configured for the given source_name

        This is particularly convenient when the user doesn't know
        the list of dimensions or the bucket name, but only the source name.

        Usage example::

            records = Dataset.from_source('telemetry').where(
                docType='main',
                submissionDate='20160701',
                appUpdateChannel='nightly'
            )
        """
        meta_bucket = 'net-mozaws-prod-us-west-2-pipeline-metadata'
        store = S3Store(meta_bucket)

        try:
            source = json.loads(store.get_key('sources.json').read())[source_name]
        except KeyError:
            raise Exception('Unknown source {}'.format(source_name))

        schema = store.get_key('{}/schema.json'.format(source['metadata_prefix'])).read()
        dimensions = [f['field_name'] for f in json.loads(schema)['dimensions']]
        return Dataset(source['bucket'], dimensions, prefix=source['prefix'])

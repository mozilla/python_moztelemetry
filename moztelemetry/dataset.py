# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import division, print_function
import functools
import heapq
import json
import random
import re
import types
from copy import copy
from inspect import isfunction
from itertools import chain
from multiprocessing import cpu_count
from six.moves import copyreg
from pyspark.sql import Row
import jmespath
from concurrent import futures
from .heka import message_parser

from .store import S3Store

DEFAULT_MAX_CONCURRENCY = int(cpu_count() * 1.5)
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


def _group_by_equal_size(obj_list, tot_groups, threshold=pow(2, 32)):
    """Partition a list of objects evenly and by file size

    Files are placed according to largest file in the smallest bucket. If the
    file is larger than the given threshold, then it is placed in a new bucket
    by itself.
    :param obj_list: a list of dict-like objects with a 'size' property
    :param tot_groups: number of partitions to split the data
    :param threshold: the maximum size of each bucket
    :return: a list of lists, one for each partition
    """
    sorted_obj_list = sorted([(obj['size'], obj) for obj in obj_list], reverse=True)
    groups = [(random.random(), []) for _ in range(tot_groups)]

    if tot_groups <= 1:
        groups = _group_by_size_greedy(obj_list, tot_groups)
        return groups
    heapq.heapify(groups)
    for obj in sorted_obj_list:
        if obj[0] > threshold:
            heapq.heappush(groups, (obj[0], [obj[1]]))
        else:
            size, files = heapq.heappop(groups)
            size += obj[0]
            files.append(obj[1])
            heapq.heappush(groups, (size, files))
    groups = [group[1] for group in groups]
    return groups


def _pickle_method(m):
    """Make instance methods pickable

    See http://stackoverflow.com/questions/1816958/cant-pickle-type-instancemethod-when-using-pythons-multiprocessing-pool-ma/1816969#1816969
    """
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


copyreg.pickle(types.MethodType, _pickle_method)


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
    new Dataset instance. The instance created will be aware of the list of
    dimensions, available on its `schema` attribute for inspection.
    """

    def __init__(self,
                 bucket,
                 schema,
                 store=None,
                 prefix=None,
                 clauses=None,
                 selection=None,
                 max_concurrency=None):
        """Initialize a Dataset provided bucket and schema

        :param bucket: bucket name
        :param schema: a list of fields describing the structure of the dataset
        :param store: an instance of S3Store, potentially reused among several
        datasets
        :param prefix: a prefix to the
        :param clauses: mapping of fields -> callables to refine the dataset
        :param max_concurrency: number of processes to spawn when collecting S3 summaries,
        defaults to 1.5 * cpu_count
        """
        self.bucket = bucket
        self.schema = schema
        self.prefix = prefix or ''
        self.clauses = clauses or {}
        self.store = store or S3Store(self.bucket)
        self.selection = selection or {}
        self.selection_compiled = {}
        self.max_concurrency = max_concurrency or DEFAULT_MAX_CONCURRENCY

    def __repr__(self):
        params = ['bucket', 'schema', 'store', 'prefix', 'clauses', 'selection', 'max_concurrency']
        stmts = ['{}={!r}'.format(param, getattr(self, param)) for param in params]
        return 'Dataset({})'.format(', '.join(stmts))

    def _copy(self,
              bucket=None,
              schema=None,
              store=None,
              prefix=None,
              clauses=None,
              selection=None,
              max_concurrency=None):
        return Dataset(
            bucket=bucket or self.bucket,
            schema=schema or self.schema,
            store=store or self.store,
            prefix=prefix or self.prefix,
            clauses=clauses or self.clauses,
            selection=selection or self.selection,
            max_concurrency=max_concurrency or self.max_concurrency)

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
        merged_properties = dict(zip(properties, properties))
        merged_properties.update(aliased_properties)

        for prop_name in (merged_properties.keys()):
            if prop_name in self.selection:
                raise Exception('The property {} has already been selected'.format(prop_name))

        new_selection = self.selection.copy()
        new_selection.update(merged_properties)

        return self._copy(selection=new_selection)

    def _compile_selection(self):
        if not self.selection_compiled:
            self.selection_compiled = dict((name, jmespath.compile(path))
                                           for name, path in self.selection.items())

    def _apply_selection(self, json_obj):
        if not self.selection:
            return json_obj

        # This is mainly for testing purposes.
        # For performance reasons the selection should be compiled
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
        return self._copy(clauses=clauses)

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

        with futures.ProcessPoolExecutor(self.max_concurrency) as executor:
            scanned = self._scan(schema, [self.prefix], clauses, executor)
        keys = sc.parallelize(scanned).flatMap(self.store.list_keys)
        return keys.take(limit) if limit else keys.collect()

    def records(self, sc, group_by='greedy', limit=None, sample=1, seed=42, decode=None, summaries=None):
        """Retrieve the elements of a Dataset

        :param sc: a SparkContext object
        :param group_by: specifies a partition strategy for the objects
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
        decode = decode or message_parser.parse_heka_message
        summaries = summaries or self.summaries(sc, limit)

        # Calculate the sample if summaries is not empty and limit is not set
        if summaries and limit is None and sample != 1:
            if sample < 0 or sample > 1:
                raise ValueError('sample must be between 0 and 1')
            print(
                "WARNING: THIS IS NOT A REPRESENTATIVE SAMPLE.\n"
                "This 'sampling' is based on s3 files and is highly\n"
                "susceptible to skew. Use only for quicker performance\n"
                "while prototyping."
            )
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
        total_size = functools.reduce(lambda acc, item: acc + item['size'], summaries, 0)
        total_size_mb = total_size / float(1 << 20)
        print("fetching %.5fMB in %s files..." % (total_size_mb, len(summaries)))

        if group_by == 'equal_size':
            groups = _group_by_equal_size(summaries, 10*sc.defaultParallelism)
        elif group_by == 'greedy':
            groups = _group_by_size_greedy(summaries, 10*sc.defaultParallelism)
        else:
            raise Exception("group_by specification is invalid")

        self._compile_selection()

        keys = (
            sc.parallelize(groups, len(groups))
            .flatMap(lambda x: x)
            .map(lambda x: x['key'])
        )
        file_handles = keys.map(self.store.get_key)

        # decode(fp: file-object) -> list[dict]
        data = file_handles.flatMap(decode)

        return data.map(self._apply_selection)

    def dataframe(self, spark, group_by='greedy', limit=None, sample=1, seed=42, decode=None, summaries=None, schema=None, table_name=None):
        """Convert RDD returned from records function to a dataframe

        :param spark: a SparkSession object
        :param group_by: specifies a paritition strategy for the objects
        :param limit: maximum number of objects to retrieve
        :param decode: an optional transformation to apply to the objects retrieved
        :param sample: percentage of results to return. Useful to return a sample
            of the dataset. This parameter is ignored when 'limit' is set.
        :param seed: initialize internal state of the random number generator (42 by default).
            This is used to make the dataset sampling reproducible. It an be set to None to obtain
            different samples.
        :param summaries: an iterable containing the summary for each item in the dataset. If None, it
            will compute calling the summaries dataset.
        :param schema: a Spark schema that overrides automatic conversion to a dataframe
        :param table_name: allows resulting dataframe to easily be queried using SparkSQL
        :return: a Spark DataFrame

        """
        rdd = self.records(spark.sparkContext, group_by, limit, sample, seed, decode, summaries)
        if not schema:
            df = rdd.map(lambda d: Row(**d)).toDF()
        else:
            df = spark.createDataFrame(rdd, schema=schema)
        if table_name:
            df.createOrReplaceTempView(table_name)
        return df

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
            source = json.loads(store.get_key('sources.json').read().decode('utf-8'))[source_name]
        except KeyError:
            raise Exception('Unknown source {}'.format(source_name))

        schema = store.get_key('{}/schema.json'.format(source['metadata_prefix'])).read().decode('utf-8')
        dimensions = [f['field_name'] for f in json.loads(schema)['dimensions']]
        return Dataset(source['bucket'], dimensions, prefix=source['prefix'])

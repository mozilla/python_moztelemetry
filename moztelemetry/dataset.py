# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
from copy import copy
from itertools import chain, islice
from multiprocessing import cpu_count
from concurrent import futures

from moztelemetry import parse_heka_message
from .store import S3Store


MAX_CONCURRENCY = int(cpu_count() * 1.5)


def group_by_size(obj_list):
    """Partitions a list of objects by cumulative size

    :param obj_list: a list of dict-like objects with a 'size' property
    :return: a list of lists, one for each partition.
    """
    threshold = 2 ** 31

    def group_reducer(acc, obj):
        curr_size, output = acc
        if curr_size + obj['size'] < threshold:
            output[-1].append(obj)
            return curr_size + obj['size'], output
        else:
            output.append([obj])
            return obj['size'], output

    return reduce(group_reducer, obj_list, (0, [[]]))[1]


class Dataset:
    """Represents a collection of objects on S3.

    A dataset can have zero, one or many filters, which are refined using the
    `where` method. The result of refining a Dataset is a Dataset itself, so
    it's possible to chain multiple `where` together.

    The actual data retrieval is triggered by the records method, which returns
    a Spark rdd containing the list of objects retrieved. To call `records`
    a SparkContext object must be provided.

    Usage example::

        bucket = 'test-bucket'
        schema = ['submission_date', 'doc_type', 'platform']

        records = Dataset(bucket, schema) \
            .where('doc_type', lambda x: x == 'main')
            .where('submission_date', lambda x: x.startswith('201607'))
            .where('dim1', lambda x: x == 'linux')
            .records(sc)
    """
    def __init__(self, bucket, schema, store=None,
        prefix=None, clauses=None):
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

    def __repr__(self):
        return ('Dataset(bucket=%s, schema=%s, store=%s, prefix=%s, clauses=%s)'
                ) % (self.bucket, self.schema, self.store, self.prefix, self.clauses)

    def where(self, dimension, condition):
        """Return a new dataset refined using the given condition

        :param dimension: the name of a dimension
        :param condition: a callable to filter the elements of a dataset.
        :return: a new dataset which elements are refined by `condition`.
        """
        if dimension in self.clauses:
            raise Exception('There should be only one clause for {}'.format(dimension))
        if dimension not in self.schema:
            raise Exception('The dimension {} doesn\'t exist'.format(dimension))
        self.clauses[dimension] = condition
        return Dataset(self.bucket, self.schema, store=self.store,
                       prefix=self.prefix, clauses=self.clauses)

    def _scan(self, dimensions, prefixes, clauses, executor):
        if not dimensions:
            return prefixes
        else:
            dimension = dimensions[0]
            clause = clauses.get(dimension)
            matched = executor.map(self.store.list_folders, prefixes, timeout=10)
            # Using chain to flatten the results of map
            matched = chain(*matched)

            if clause:
                matched = (x for x in matched if clause(x.strip('/').split('/')[-1]))

            return self._scan(dimensions[1:], matched, clauses, executor)

    def _summaries(self):
        clauses = copy(self.clauses)
        schema = self.schema

        if self.prefix:
            schema = ['prefix'] + schema
            clauses['prefix'] = lambda x: x == self.prefix

        with futures.ProcessPoolExecutor(MAX_CONCURRENCY) as executor:
            scanned = self._scan(schema, [self.prefix], clauses, executor)
            keys = executor.map(self.store.list_keys, scanned)
        # Using chain to keep the list of keys as a generator
        return chain(*keys)

    def records(self, sc, limit=None, decode=parse_heka_message):
        """Retrieve the elements of a dataset

        :param sc: a SparkContext object
        :param limit: maximum number of objects to retrieve
        :param decode: an optional transformation to apply to the objects
        retrieved
        :return: a Spark rdd containing the elements retrieved
        """
        summaries = self._summaries()
        if limit:
            summaries = islice(summaries, limit)
        groups = group_by_size(summaries)

        return sc.parallelize(groups, len(groups)) \
            .flatMap(lambda x:x) \
            .map(lambda x: self.store.get_key(x['key'])) \
            .map(lambda x: decode(x))

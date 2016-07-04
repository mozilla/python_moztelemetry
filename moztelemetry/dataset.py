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
    def __init__(self, bucket, schema, store=None,
        prefix=None, clauses=None):
        self.bucket = bucket
        self.schema = schema
        self.prefix = prefix or ''
        self.clauses = clauses or {}
        self.store = store or S3Store(self.bucket)

    def where(self, dimension, condition):
        if dimension in self.clauses:
            raise Exception(
                'There should be only one clause for {}'.format(dimension))
        if dimension not in self.schema:
            raise Exception(
                'The dimension {} doesn\'t exist'.format(dimension)
            )
        self.clauses[dimension] = condition
        return Dataset(self.bucket, self.schema, store=self.store,
                       prefix=self.prefix, clauses=self.clauses)

    def _scan(self, dimensions, prefixes, clauses, executor):
        if not dimensions:
            return prefixes
        else:
            dimension = dimensions[0]
            clause = clauses.get(dimension)
            matched = executor.map(self._get_folders, prefixes, timeout=10)
            # Using chain to keep the list of folders as a generator
            matched = chain(*list(matched))
            if clause:
                matched = filter(lambda x: clause(x.strip('/').split('/')[-1]),
                                 matched)
            return self._scan(dimensions[1:], matched, clauses, executor)

    def _list_keys(self, folder):
        # store = self.store_type(self.bucket)
        return list(self.store.list_keys(folder))

    def _get_folders(self, prefix):
        # store = self.store_type(self.bucket)
        return list(self.store.list_folders(prefix))


    def summaries(self, limit=None):
        clauses = copy(self.clauses)
        schema = self.schema

        if self.prefix:
            schema = ['prefix'] + schema
            clauses['prefix'] = lambda x: x == self.prefix

        with futures.ProcessPoolExecutor(MAX_CONCURRENCY) as executor:
            scanned = self._scan(schema, [self.prefix], clauses, executor)
            keys = executor.map(self._list_keys, scanned)
        # Using chain to keep the list of keys as a generator
        keys = chain(*keys)
        return islice(keys, limit) if limit else keys

    def records(self, sc, limit=None, decode=None):
        groups = group_by_size(self.summaries(limit))

        if decode is None:
            decode = parse_heka_message

        return sc.parallelize(groups, len(groups)) \
            .flatMap(lambda x:x) \
            .map(lambda x: self.store.get_key(x['key'])) \
            .map(lambda x: decode(x))

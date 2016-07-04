# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
from moztelemetry import parse_heka_message
from moztelemetry.store import S3Store
from itertools import chain, imap, islice


flat_map = lambda f, items: chain.from_iterable(imap(f, items))

def group_by_size(obj_list):
    threshold = 2**31

    def group_reducer(acc, obj):
        curr_size, output = acc
        if curr_size + obj.size < threshold:
            output[-1].append(obj)
            return curr_size + obj.size, output
        else:
            output.append([obj])
            return obj.size, output

    return reduce(group_reducer, obj_list, [[]])


    return reduce(group_reducer, obj_list, [])[1]

class Dataset:

    def __init__(self, bucket, schema, prefix, clauses, store=None):
        self.bucket = bucket
        self.schema = schema
        self.prefix = prefix
        self.clauses = clauses
        if not store:
            self.store = S3Store(self.bucket)
        else:
            self.store = store

    def where(self, dimension, condition):
        if dimension in self.clauses:
            raise Exception(
                'There should be only one clause for {}'.format(dimension)
            )
        if dimension not in self.schema:
            raise Exception(
                'The dimension {} doesn\'t exist'.format(dimension)
            )
        new_clauses = dict(**self.clauses)
        new_clauses[dimension] = condition
        return Dataset(self.bucket, self.schema, self.prefix,
                       new_clauses, self.store)

    def _scan(self, dimensions, prefixes):
        if not dimensions:
            return prefixes
        else:
            dimension = dimensions[0]
            clause = self.clauses.get(dimension)
            matched = flat_map(lambda x: self.store.list_folders(x), prefixes)
            if clause:
                matched = filter(lambda x: clause(x.strip('/').split('/')[-1]),
                                 matched)
            return self._scan(dimensions[1:], matched)

    def summaries(self, limit=None):
        scanned = self._scan(self.schema, [self.prefix])
        keys = flat_map(lambda x: self.store.list_keys(x), scanned)
        if limit:
            return islice(keys, limit)
        return keys

    def records(self, sc, limit=None, decode=None):
        groups = group_by_size(self.summaries(limit))

        if decode is None:
            decode = parse_heka_message

        return sc.parallelize(groups, len(groups)
        ).flatMap(lambda x: x
        ).flatMap(lambda x: decode(self.store.get_key(x.key)))

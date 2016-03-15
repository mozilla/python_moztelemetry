#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

""" SimpleDB backed filter service for telemetry submissions.

Example usage:
sdb = SDB("telemetry_v4")
sdb.query(submissionDate=("20150415", "20150416"), appName="Firefox", appBuildID=("20150414000000", "20150414999999"))

"""

import ujson as json
import traceback
import boto.sdb
import argparse
import boto
import sys

from collections import defaultdict
from datetime import datetime
from dateutil.relativedelta import relativedelta
from boto.s3.connection import S3Connection
from multiprocessing.pool import ThreadPool
from joblib import Parallel, delayed
from telemetry.telemetry_schema import TelemetrySchema
from cStringIO import StringIO

METADATA_BUCKET = "net-mozaws-prod-us-west-2-pipeline-metadata"
S3_DEFAULT_ENDPOINT = "s3-us-west-2.amazonaws.com"


class SDB:
    def __init__(self, prefix, months_retention=12, read_only=True):
        self.sdb = boto.sdb.connect_to_region('us-west-2')
        self.read_only = read_only
        self.prefix = prefix

        # Get existing domains
        domains_existing = map(lambda x: x.name, self.sdb.get_all_domains())
        domains_existing = set(filter(lambda x: x.startswith(prefix), domains_existing))

        # Use a threadpool to execute SimpleDB calls asynchronously
        self._pool = ThreadPool(1000)

        if not read_only:
            # Generate domain names that we want to keep
            now = datetime.now()
            first = now - relativedelta(months=months_retention - 1)
            domains = set()

            # Create also future domains as this will run on a periodic basis, even if we are using an AWS lambda
            # function, to ensure correctness. Furthermore we don't want to create domains within the lambda function.
            for i in range(months_retention + 12):
                domains.add("{}_{}".format(prefix, (first + relativedelta(months=i)).strftime("%Y%m")))

            # Remove older domains
            for domain_name in domains_existing.difference(domains):
                self.sdb.delete_domain(domain_name)

            # Create new domains
            for domain_name in domains.difference(domains_existing):
                self.sdb.create_domain(domain_name)
        else:
            domains = domains_existing

        # Cache domains
        self._domains = {}
        for domain_name in domains:
            self._domains[domain_name[len(self.prefix) + 1:]] = self.sdb.get_domain(domain_name)

    def __del__(self):
        # We terminate rather than closing because if we're destroying the SDB object,
        # then the workers' results won't matter anyways.
        # Although this is called automatically when self._pool goes out of scope,
        # we also need to join() below
        self._pool.terminate()

        # Wait for the thread pool to terminate;
        # this is needed in order to make sure all the processes are dead when the SDB goes out of scope.
        self._pool.join()

    def __contains__(self, name):
        return name in self._domains

    def __getitem__(self, name):
        return self._domains.get(name, None)

    def get_daily_stats(self, from_date, to_date):
        from_date = datetime.strptime(from_date, "%Y%m%d")
        to_date = datetime.strptime(to_date, "%Y%m%d")
        jobs = []

        n_days = int((to_date - from_date).total_seconds()/(24*60*60))
        for i in range(n_days + 1):
            submission_date = (from_date + relativedelta(days=i)).strftime("%Y%m%d")
            domain = self[submission_date[:-2]]
            query = "select count(*) from `{}` where submissionDate='{}'".format(domain.name, submission_date)
            jobs.append(self._pool.apply_async(lambda d, q, s: (s, sum([int(c["Count"]) for c in d.select(q)])), [domain, query, submission_date]))

        return dict([job.get() for job in jobs])

    def diff_stats(self, previous_stats, current_stats):
        days = set(current_stats.keys() + previous_stats.keys())

        for day in sorted(days):
            prev = previous_stats.get(day, 0)
            current = current_stats.get(day, 0)
            diff = current - prev
            diff_perc = 100.0*diff/current if current else float('nan')
            print "Day {} - previous: {}, current: {}, added: {} ({}% missing)".format(day, prev, current, diff, diff_perc)

    def print_lambda_stats(self, from_date, to_date):
        from_date = datetime.strptime(from_date, "%Y%m%d")
        to_date = datetime.strptime(to_date, "%Y%m%d")
        jobs = []

        n_days = int((to_date - from_date).total_seconds()/(24*60*60))
        for i in range(n_days + 1):
            submission_date = (from_date + relativedelta(days=i)).strftime("%Y%m%d")
            domain = self[submission_date[:-2]]
            lambda_query = "select count(*) from `{}` where submissionDate='{}' and lambda='true'".format(domain.name, submission_date)
            total_query = "select count(*) from `{}` where submissionDate='{}'".format(domain.name, submission_date)

            n_lambda = sum([int(c["Count"]) for c in domain.select(lambda_query)])
            n_total = sum([int(c["Count"]) for c in domain.select(total_query)])
            n_missing = 100.0*(n_total - n_lambda)/n_total if n_total else float('nan')

            print "Day {} - lambda {}, total {}, ({}% missing)".format(submission_date, n_lambda, n_total, n_missing)

    def query(self, **kwargs):
        # Get list of domains for selected submission_date
        submission_date = kwargs.get("submissionDate", None)

        if isinstance(submission_date, basestring):
            submission_date = (submission_date, submission_date)

        if submission_date is None:
            domains = self._domains.values()
        elif isinstance(submission_date, tuple) and len(submission_date) == 2:
            try:
                begin = datetime.strptime(submission_date[0], "%Y%m%d")
                end = datetime.strptime(submission_date[1], "%Y%m%d")
            except ValueError:
                raise ValueError("Invalid submissionDate format")

            query_domains = set()
            query_domains.add("{}".format(end.strftime("%Y%m")))
            diff = relativedelta(end, begin)

            for i in range(diff.months + 1):
                query_domains.add("{}".format((begin + relativedelta(months=i)).strftime("%Y%m")))

            domains = [self[domain] for domain in query_domains if domain in self]
        else:
            raise ValueError("Invalid submissionDate format")

        if not domains:
            return []

        # Build query
        kwargs = {k: v for k, v in kwargs.iteritems() if v is not None}  # 'None' values match everything
        partial_query = " and ".join([self._sqlize(k, v) for k, v in kwargs.iteritems()])
        jobs = []

        for domain in domains:
            query = "select submissionDate from `{}` where {}".format(domain.name, partial_query)
            # Not sure why closing over query and domain generates empty results...
            jobs.append(self._pool.apply_async(lambda d, q: [f.name for f in d.select(q)], [domain, query]))

        return reduce(lambda x, y: x + y, [job.get() for job in jobs])

    def _sqlize(self, attribute, value):
        if isinstance(value, tuple) and isinstance(value[0], basestring) and isinstance(value[1], basestring):
            return "{} >= '{}' and {} <= '{}'".format(attribute, value[0], attribute, value[1])
        elif isinstance(value, basestring):
            return "{} = '{}'".format(attribute, value)
        else:
            raise ValueError("An attribute should be represented by either a tuple of strings or a string")

    def batch_put(self, domain_name, attributes):
        if self.read_only:
            raise NotImplementedError("Method not available in read-only mode")

        domain = self[domain_name]
        assert(domain)
        self._pool.apply_async(lambda: domain.batch_put_attributes(attributes))

    def flush_put(self):
        if self.read_only:
            raise NotImplementedError("Method not available in read-only mode")

        self._pool.close()
        self._pool.join()
        self._pool = ThreadPool(1000)


class BatchPut:
    def __init__(self, sdb):
        self._cache = defaultdict(dict)
        self._max = 25  # 25 is the limit for SimpleDB
        self._sdb = sdb

    def put(self, domain_name, filename, attributes):
        domain = self._cache[domain_name]
        domain[filename] = attributes

        if len(domain) == self._max:
            self._sdb.batch_put(domain_name, domain)
            self._cache[domain_name] = {}

    def flush(self):
        for domain_name, entries in self._cache.iteritems():
            self._sdb.batch_put(domain_name, entries)

        self._sdb.flush_put()
        self._cache = defaultdict(dict)


def delta_ms(start, end=None):
    if end is None:
        end = datetime.now()

    delta = end - start
    return delta.seconds * 1000.0 + float(delta.microseconds) / 1000.0


def delta_sec(start, end=None):
    return delta_ms(start, end) / 1000.0


def update_published_v4_files(sdb, bucket, bucket_prefix, submission_date, limit=None):
    conn = boto.connect_s3(host=S3_DEFAULT_ENDPOINT)
    metadata = conn.get_bucket(METADATA_BUCKET, validate=False)
    schema_key = metadata.get_key("{}/schema.json".format(bucket_prefix))
    schema_string = schema_key.get_contents_as_string()
    schema = TelemetrySchema(json.loads(schema_string))
    bucket = conn.get_bucket(bucket, validate=False)

    added_count = 0
    total_count = 0
    start_time = datetime.now()
    done = False
    last_key = ''
    batch = BatchPut(sdb)
    prefix = "{}/{}".format(bucket_prefix, submission_date) if submission_date else bucket_prefix

    print "Bucket: {} - Prefix: {} - Date: {}".format(bucket.name, bucket_prefix, submission_date)

    while not done:
        try:
            for key in bucket.list(marker=last_key, prefix=prefix):
                last_key = key.name

                if total_count % 1e5 == 0:
                    print("Looked at {} total records in {} seconds, added {}".
                            format(total_count, delta_sec(start_time), added_count))

                dims = schema.get_dimension_map(schema.get_dimensions(".", key.name[len(bucket_prefix) + 1:], dirs_only=True))

                if (dims["submissionDate"] == submission_date) and dims["submissionDate"][:-2] in sdb:
                    batch.put(dims["submissionDate"][:-2], key.name, dims)
                    added_count += 1

                total_count += 1
                if total_count == limit:
                    done = True
                    break

        except Exception as e:
            print("Error listing keys: {}".format(e))
            traceback.print_exc()
            print("Continuing from last seen key: {}".format(last_key))
            continue

        break

    batch.flush()
    print("Overall, added {} of {} in {} seconds".format(added_count, total_count, delta_sec(start_time)))


def update(dataset, submission_date, limit=None):
    if limit:
        limit = int(limit)

    if dataset not in ["telemetry", "telemetry-release"]:
        raise ValueError("Unsupported dataset")

    conn = boto.connect_s3(host=S3_DEFAULT_ENDPOINT)
    meta_bucket = conn.get_bucket(METADATA_BUCKET, validate=False)
    sources = json.loads(meta_bucket.get_key("sources.json").get_contents_as_string())
    bucket = sources[dataset]["bucket"]
    prefix = sources[dataset]["prefix"]

    if prefix == "telemetry-2":
        sdb = SDB("telemetry_v4", read_only=False)  # Backwards compatibility
    else:
        sdb = SDB(prefix, read_only=False)

    prev = sdb.get_daily_stats(submission_date, submission_date)
    update_published_v4_files(sdb, bucket, prefix, submission_date=submission_date, limit=limit)
    curr = sdb.get_daily_stats(submission_date, submission_date)

    print "Filter service stats:"
    print "Note that the following numbers are correct only if there isn't another entity concurrently pushing new submissions:"
    sdb.diff_stats(prev, curr)

    print "AWS lambda stats:"
    sdb.print_lambda_stats(submission_date, submission_date)


def wrap_streams_update(*args, **kwargs):
    buffer = StringIO()
    sys.stdout = buffer
    sys.stderr = buffer
    update(*args, **kwargs)
    return buffer.getvalue()


def main(dataset, from_date, to_date, limit=None):
    from_date = datetime.strptime(from_date, "%Y%m%d")
    to_date = datetime.strptime(to_date, "%Y%m%d")

    dates = []
    for i in range((to_date - from_date).days + 1):
        dates.append((from_date + relativedelta(days=i)).strftime("%Y%m%d"))

    out = Parallel(n_jobs=-1, backend="multiprocessing")(delayed(wrap_streams_update)(limit=limit, dataset=dataset, submission_date=d) for d in dates)

    for o in out:
        print o


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a SimpleDB based index for telemetry submissions on S3",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-l", "--limit", help="Maximum number of files to index", default=None)
    parser.add_argument("-d", "--dataset", help="Dataset name", default="telemetry")
    parser.add_argument("-f", "--from-date", help="Add only telemetry files submitted after this date (included)", default=None)
    parser.add_argument("-t", "--to-date", help="Add only telemetry files submitted before this date (included)", default=None)

    args = parser.parse_args()
    main(args.dataset, args.from_date, args.to_date, args.limit)

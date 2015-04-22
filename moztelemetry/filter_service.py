#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

""" SimpleDB backed filter service for telemetry submissions.

Example usage:
sdb = SDB("telemetry_v2")
sdb.query(submissionDate=("20150415", "20150416"), appName="Firefox", appBuildID=("20150414000000", "20150414999999"))

"""

import ujson as json
import traceback
import boto.sdb
import argparse
import signal

from collections import defaultdict
from datetime import datetime
from dateutil.relativedelta import relativedelta
from boto.s3.connection import S3Connection
from multiprocessing.pool import ThreadPool
from telemetry.telemetry_schema import TelemetrySchema

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
            last = datetime.now()
            first = last + relativedelta(months=-(months_retention - 1))
            domains = set()

            for i in range(months_retention):
                domains.add("{}_{}".format(prefix, (first + relativedelta(months=i)).strftime("%Y%m")))

            # Remove older domains
            for domain_name in domains_existing.difference(domains):
                self.sdb.delete_domain(domain_name)

            for domain_name in domains.difference(domains_existing):
                self.sdb.create_domain(domain_name)
        else:
            domains = domains_existing

        # Cache domains
        self._domains = {}
        for domain_name in domains:
            self._domains[domain_name[len(self.prefix) + 1:]] = self.sdb.get_domain(domain_name)

    def __contains__(self, name):
        return name in self._domains

    def __getitem__(self, name):
        return self._domains.get(name, None)

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
            query = "select submissionDate from {} where {}".format(domain.name, partial_query)
            # Not sure why closing over query generates empty results...
            jobs.append(self._pool.apply_async(lambda x: [f.name for f in domain.select(x)], [query]))

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


def delta_ms(start, end=None):
    if end is None:
        end = datetime.now()

    delta = end - start
    return delta.seconds * 1000.0 + float(delta.microseconds) / 1000.0


def delta_sec(start, end=None):
    return delta_ms(start, end) / 1000.0

def update_published_v2_files(sdb, from_submission_date=None, limit=None):
    s3 = S3Connection()
    bucket_name = "telemetry-published-v2"
    bucket = s3.get_bucket(bucket_name)
    schema_key = bucket.get_key("telemetry_schema.json")
    schema_string = schema_key.get_contents_as_string()
    schema = TelemetrySchema(json.loads(schema_string))

    termination_requested = [False]
    def keyboard_interrupt_handler(signal, frame):
        termination_requested[0] = True
    signal.signal(signal.SIGINT, keyboard_interrupt_handler)

    added_count = 0
    total_count = 0
    start_time = datetime.now()
    current_batch = defaultdict(dict)
    done = False
    last_key = ''

    while not done:
        try:
            for key in bucket.list(marker=last_key):
                last_key = key.name

                if total_count % 25 == 0:  # 25 is the limit for SimpleDB
                    insert_published_files_batch(sdb, current_batch)
                    current_batch = defaultdict(dict)

                if total_count % 1e5 == 0:
                    print("Looked at {} total records in {} seconds, added {}".
                          format(total_count, delta_sec(start_time), added_count))

                dims = schema.get_dimension_map(schema.get_dimensions(".", key.name))

                if (from_submission_date is None or dims["submission_date"] >= from_submission_date) and \
                   dims["submission_date"][:-2] in sdb and dims["reason"] != "idle_daily":
                    domain = current_batch[dims["submission_date"][:-2]]
                    domain[key.name] = {"reason": dims.get("reason"),
                                        "appName": dims.get("appName"),
                                        "appUpdateChannel": dims.get("appUpdateChannel"),
                                        "appVersion": dims.get("appVersion"),
                                        "appBuildID": dims.get("appBuildID"),
                                        "submissionDate": dims.get("submission_date")}
                    added_count += 1

                total_count += 1
                if total_count == limit or termination_requested[0]:
                    done = True
                    break

        except Exception as e:
            print("Error listing keys: {}".format(e))
            traceback.print_exc()
            print("Continuing from last seen key: {}".format(last_key))

        break

    insert_published_files_batch(sdb, current_batch)
    sdb.flush_put()
    print("Overall, added {} of {} in {} seconds".format(added_count, total_count, delta_sec(start_time)))


def insert_published_files_batch(sdb, batch):
    for domain_name, items in batch.iteritems():
        sdb.batch_put(domain_name, items)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a SimpleDB based index for telemetry submissions on S3",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-l", "--limit", help="Maximum number of files to index", default=None)
    parser.add_argument("-s", "--schema-version", help="Telemetry schema version", default="v2")
    parser.add_argument("-f", "--from-date", help="Add only telemetry files submitted after this date (included)", default=None)

    args = parser.parse_args()

    if args.limit:
        args.limit = int(args.limit)

    if args.schema_version != "v2":
        raise ValueError("Unsupported schema version")

    sdb = SDB("telemetry_v2", read_only=False)
    update_published_v2_files(sdb, from_submission_date=args.from_date, limit=args.limit)

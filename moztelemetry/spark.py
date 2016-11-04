#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

""" This module implements the Telemetry API for Spark.

Example usage:
pings = get_pings(None, app="Firefox", channel="nightly", build_id=("20140401000000", "20140402999999"), reason="saved_session")
histories = get_clients_history(sc, fraction = 0.01)

"""

import boto
from functools import partial
import json as json
import logging
import numpy.random as random
import ssl

from histogram import Histogram
from heka_message_parser import parse_heka_message
from xml.sax import SAXParseException
from telemetry.telemetry_schema import TelemetrySchema
import telemetry.util.s3 as s3u
from .dataset import Dataset


logger = logging.getLogger(__name__)

if not boto.config.has_section('Boto'):
    boto.config.add_section('Boto')
boto.config.set('Boto', 'http_socket_timeout', '10')  # https://github.com/boto/boto/issues/2830

_chunk_size = 100*(2**20)
try:
    _conn = boto.connect_s3(host="s3-us-west-2.amazonaws.com")

    _bucket = _conn.get_bucket("net-mozaws-prod-us-west-2-pipeline-data", validate=False)
    _bucket_meta = _conn.get_bucket("net-mozaws-prod-us-west-2-pipeline-metadata", validate=False)
except:
    pass  # Handy for testing purposes...

_sources = None


def get_clients_history(sc, **kwargs):
    """ Returns RDD[client_id, ping]. This API is experimental and might change entirely at any point!
    """

    fraction = kwargs.pop("fraction", 1.0)

    if fraction < 0 or fraction > 1:
        raise ValueError("Invalid fraction argument")

    if kwargs:
        raise TypeError("Unexpected **kwargs {}".format(repr(kwargs)))

    clients = [x.name for x in list(_bucket.list(prefix="telemetry_sample_42/", delimiter="/"))]

    if clients and fraction != 1.0:
        sample = random.choice(clients, size=len(clients)*fraction, replace=False)
    else:
        sample = clients

    client_ids = [client_prefix[20:-1] for client_prefix in sample]
    parallelism = max(len(sample), sc.defaultParallelism)

    return sc.parallelize(zip(client_ids, sample), parallelism).\
              partitionBy(len(sample)).\
              flatMapValues(_get_client_history).\
              filter(lambda x: x[1] is not None).\
              flatMapValues(lambda x: _read(x))


def get_pings(sc, app=None, build_id=None, channel=None, doc_type='saved_session',
              fraction=1.0, schema=None, source_name='telemetry', source_version='4',
              submission_date=None, version=None):
    """ Returns a RDD of Telemetry submissions for a given filtering criteria.

    :param app: an application name, e.g.: "Firefox"
    :param channel: a channel name, e.g.: "nightly"
    :param version: the application version, e.g.: "40.0a1"
    :param build_id: a build_id or a range of build_ids, e.g.:
                     "20150601000000" or ("20150601000000", "20150610999999")
    :param submission_date: a submission date or a range of submission dates, e.g:
                            "20150601" or ("20150601", "20150610")
    :param source_name: source name, set to "telemetry" by default
    :param source_version: source version, set to "4" by default
    :param doc_type: ping type, set to "saved_session" by default
    :param fraction: the fraction of pings to return, set to 1.0 by default
    """
    if schema:
        print ("The 'schema' parameter is deprecated. "
               "Version 4 is now the only schema supported.")
        if schema != "v4":
            raise ValueError("Invalid schema version")

    dataset = Dataset.from_source('telemetry')

    filters = (
        ('docType', doc_type),
        ('sourceName', source_name),
        ('sourceVersion', source_version),
        ('appName', app),
        ('appUpdateChannel', channel),
        ('appVersion', version),
    )

    for key, condition in filters:
        if condition and condition != '*':
            dataset = dataset.where(**{key: condition})

    # build_id and submission_date can be either strings or tuples or lists,
    # so they deserve a special treatment
    special_cases = dict(appBuildId=build_id, submissionDate=submission_date)

    def range_compare(min_val, max_val, val):
        return min_val <= val <= max_val

    for key, value in special_cases.items():
        if value is not None and value != '*':
            if isinstance(value, basestring):
                condition = value
            elif type(value) in (list, tuple) and len(value) == 2:
                start, end = value
                condition = partial(range_compare, start, end)
            else:
                raise ValueError(('{} must be either a string or a 2 elements '
                                  'list/tuple'. format(key)))
            dataset = dataset.where(**{key: condition})

    return dataset.records(sc, sample=fraction)


def get_pings_properties(pings, paths, only_median=False, with_processes=False,
                         histograms_url=None, additional_histograms=None):
    """
    Returns a RDD of a subset of properties of pings. Child histograms are
    automatically merged with the parent histogram.

    If one of the paths points to a keyedHistogram name without supplying the
    actual key, returns a dict of all available subhistograms for that property.

    :param with_processes: should separate parent and child histograms be
                           included as well?
    :param paths: paths to properties in the payload, with levels separated by "/".
                  These can be supplied either as a list, eg.
                  ["application/channel", "payload/info/subsessionStartDate"],
                  or as the values of a dict keyed by custom identifiers, eg.
                  {"channel": "application/channel", "ssd": "payload/info/subsessionStartDate"}.
    :param histograms_url: see histogram.Histogram constructor
    :param additional_histograms: see histogram.Histogram constructor

    The returned RDD contains a dict for each ping with the required properties as values,
    keyed by the original paths (if 'paths' is a list) or the custom identifier keys
    (if 'paths' is a dict).
    """
    if type(pings.first()) == str:
        pings = pings.map(lambda p: json.loads(p))

    if type(paths) == str:
        paths = [paths]

    # Use '/' as dots can appear in keyed histograms
    if type(paths) == dict:
        paths = [(prop_name, path.split("/")) for prop_name, path in paths.iteritems()]
    else:
        paths = [(path, path.split("/")) for path in paths]

    return pings.map(lambda p: _get_ping_properties(p, paths, only_median,
                                                    with_processes,
                                                    histograms_url,
                                                    additional_histograms)) \
                .filter(lambda p: p)


def get_one_ping_per_client(pings):
    """
    Returns a single ping for each client in the RDD. This operation is expensive
    as it requires data to be shuffled around. It should be run only after extracting
    a subset with get_pings_properties.
    """
    return _get_one_ping_per_client(pings, lambda p1, p2: p1)


def get_newest_ping_per_client(pings):
    """
    Returns the newest ping of each client in the RDD based on the submission
    date. The caveats of get_one_ping_per_client apply here as well.
    """
    submission_date = "meta/submissionDate"
    reduce_func = lambda p1, p2:\
        p1 if p1[submission_date] > p2[submission_date] else p2
    return _get_one_ping_per_client(pings, reduce_func)


def get_records(sc, source_name, **kwargs):
    """ Returns a RDD of records for a given data source and filtering criteria.

    All data sources support
    :param fraction: the fraction of files read from the data source, set to 1.0
                     by default.

    Depending on the data source, different filtering criteria will be available.

    Filtering criteria should be specified using the TelemetrySchema approach:
    Range filter: submissionDate={"min": "20150901", "max": "20150908"}
                  or submissionDate=("20150901", "20150908")
    List filter: appUpdateChannel=["nightly", "aurora", "beta"]
    Value filter: docType="main"

    """
    schema = _get_source_schema(source_name)
    if schema is None:
        raise ValueError("Error getting schema for {}".format(source_name))

    bucket_name = _sources[source_name]["bucket"]
    bucket_prefix = _sources[source_name]["prefix"]
    if bucket_prefix[-1] != "/":
        bucket_prefix = bucket_prefix + "/"

    fraction = kwargs.pop("fraction", 1.0)
    if fraction < 0 or fraction > 1:
        raise ValueError("Invalid fraction argument")

    filter_args = {}
    field_names = [f["field_name"] for f in schema["dimensions"]]
    for field_name in field_names:
        field_filter = kwargs.pop(field_name, None)
        if field_filter is None:
            continue
        # Special case for compatibility with get_pings:
        #   If we get a filter parameter that is a tuple of length 2, treat it
        #   as a min/max filter instead of a list of allowed values.
        if isinstance(field_filter, tuple) and len(field_filter) == 2:
            field_filter = {"min": field_filter[0], "max": field_filter[1]}
        filter_args[field_name] = field_filter

    if kwargs:
        raise TypeError("Unexpected **kwargs {}".format(repr(kwargs)))

    # We should eventually support data sources in other buckets, but for now,
    # assume that everything lives in the v4 data bucket.
    assert(bucket_name == _bucket.name)
    # TODO: cache the buckets, or at least recognize the ones we already have
    #       handles to (_bucket_* vars)
    bucket = _bucket # TODO: bucket = _conn.get_bucket(bucket_name, validate=False)
    filter_schema = _filter_to_schema(schema, filter_args)
    files = _list_s3_filenames(bucket, bucket_prefix, filter_schema)

    if files and fraction != 1.0:
        sample = random.choice(files, size=len(files)*fraction, replace=False)
    else:
        sample = files

    parallelism = max(len(sample), sc.defaultParallelism)
    return sc.parallelize(sample, parallelism).flatMap(_read)


def _get_data_sources():
    try:
        key = _bucket_meta.get_key("sources.json")
        sources = key.get_contents_as_string()
        return json.loads(sources)
    except ssl.SSLError:
        return {}


def _get_source_schema(source_name):
    global _sources
    if _sources is None:
        _sources = _get_data_sources()

    if source_name not in _sources:
        raise ValueError("Unknown data source: {}. Known sources: [{}].".format(source_name, ", ".join(sorted(_sources.keys()))))

    if "schema" not in _sources[source_name]:
        try:
            key = _bucket_meta.get_key("{}/schema.json".format(_sources[source_name].get("metadata_prefix", source_name)))
            schema = key.get_contents_as_string()
            _sources[source_name]["schema"] = json.loads(schema)
        except ssl.SSLError:
            return None
    return _sources[source_name]["schema"]


def _list_s3_filenames(bucket, prefix, schema):
    return [k.name for k in s3u.list_heka_partitions(bucket, prefix, schema=schema)]


def _filter_to_schema(schema, filter_args):
    new_schema = {"version": 1, "dimensions": []}
    for i, dim in enumerate(schema["dimensions"]):
        new_filter = {
            "field_name": schema["dimensions"][i].get("field_name", "field{}".format(i)),
            "allowed_values": "*"
        }
        if dim["field_name"] in filter_args:
            new_filter["allowed_values"] = filter_args[dim["field_name"]]
        new_schema["dimensions"].append(new_filter)
    return TelemetrySchema(new_schema)


def _get_client_history(client_prefix):
    try:
        return [x.name for x in list(_bucket.list(prefix=client_prefix))]
    except SAXParseException:  # https://groups.google.com/forum/#!topic/boto-users/XCtTFzvtKRs
        return None


def _read(filename):
    try:
        key = _bucket.get_key(filename)

        if key is None:
            # In some rare cases it's not possible to retrieve the content of a
            # file (see bug 1282441 for more details).
            # Let's log the problem and move on.
            logger.error("File not found: {}".format(filename))
            return []

        key.open_read()
        return parse_heka_message(key)
    except ssl.SSLError:
        return []


def _get_ping_properties(ping, paths, only_median, with_processes,
                         histograms_url, additional_histograms):
    result = {}

    for property_name, path in paths:
        cursor = ping

        if path[0] == "payload":
            path = path[1:]  # Translate v4 histogram queries to v2 ones
            cursor = ping.get("payload", None)

            if cursor is None:
                return

        if path[0] == "histograms" or path[0] == "keyedHistograms":
            if path[0] == "keyedHistograms" and len(path) == 2:
                # Include histograms for all available keys.
                # These are returned as a subdict mapped from the property_name.
                try:
                    kh_keys = set(cursor["keyedHistograms"][path[1]].keys())
                    if isinstance(cursor, dict):
                        content_kh = cursor.get("processes", {}) \
                                           .get("content", {}) \
                                           .get("keyedHistograms", None)
                        if content_kh is not None:
                            kh_keys.update(content_kh[path[1]].keys())
                        else:
                            for payload in cursor.get("childPayloads", []):
                                kh_keys.update(payload["keyedHistograms"][path[1]].keys())
                except:
                    result[property_name] = None
                    continue

                if kh_keys:
                    kh_histograms = {}
                    for kh_key in kh_keys:
                        props = _get_merged_histograms(cursor, kh_key,
                                                       path + [kh_key],
                                                       with_processes,
                                                       histograms_url,
                                                       additional_histograms)
                        for k, v in props.iteritems():
                            kh_histograms[k] = v.get_value(only_median) if v else None

                    result[property_name] = kh_histograms
                else:
                    # No available subhistograms.
                    result[property_name] = None
            else:
                props = _get_merged_histograms(cursor, property_name, path,
                                               with_processes, histograms_url,
                                               additional_histograms)
                for k, v in props.iteritems():
                    result[k] = v.get_value(only_median) if v else None
        else:
            prop = _get_ping_property(cursor, path, histograms_url,
                                      additional_histograms)
            result[property_name] = prop

    return result


def _get_ping_property(cursor, path, histograms_url, additional_histograms):
    is_histogram = False
    is_keyed_histogram = False

    if path[0] == "histograms":
        is_histogram = True
    elif path[0] == "keyedHistograms":
        # Deal with histogram names that contain a slash...
        path = path[:2] + (["/".join(path[2:])] if len(path) > 2 else [])
        is_keyed_histogram = True

    try:
        for field in path:
            cursor = cursor[field]
    except:
        return None

    if cursor is None:
        return None
    if is_histogram:
        return Histogram(path[-1], cursor, histograms_url=histograms_url,
                         additional_histograms=additional_histograms)
    elif is_keyed_histogram:
        histogram = Histogram(path[-2], cursor, histograms_url=histograms_url,
                              additional_histograms=additional_histograms)
        histogram.name = "/".join(path[-2:])
        return histogram
    else:
        return cursor


def _get_merged_histograms(cursor, property_name, path, with_processes,
                           histograms_url, additional_histograms):
    if path[0] == "histograms" and len(path) != 2:
        raise ValueError("Histogram access requires a histogram name.")
    elif path[0] == "keyedHistograms" and len(path) != 3:
        raise ValueError("Keyed histogram access requires both a histogram name and a label.")

    # Get parent property
    parent = _get_ping_property(cursor, path, histograms_url,
                                additional_histograms)

    # Get children properties
    if not isinstance(cursor, dict):
        children = []
    else:
        children = [_get_ping_property(cursor.get("processes", {}) \
                                             .get("content", {}),
                                       path, histograms_url,
                                       additional_histograms)]
        children += [_get_ping_property(child, path, histograms_url,
                                        additional_histograms) \
                        for child in cursor.get("childPayloads", {})]
        children = filter(lambda h: h is not None, children)

    # Merge parent and children
    merged = ([parent] if parent else []) + children

    result = {}
    if with_processes:
        result[property_name + "_parent"] = parent
        result[property_name + "_children"] = reduce(lambda x, y: x + y, children) if children else None
    result[property_name] = reduce(lambda x, y: x + y, merged) if merged else None

    return result


def _get_one_ping_per_client(pings, reduceFunc):
    if type(pings.first()) == str:
        pings = pings.map(lambda p: json.loads(p))

    filtered = pings.filter(lambda p: "clientID" in p or "clientId" in p)

    if not filtered:
        raise ValueError("Missing clientID/clientId attribute.")

    if "clientID" in filtered.first():
        client_id = "clientID"  # v2
    else:
        client_id = "clientId"  # v4

    return filtered.map(lambda p: (p[client_id], p)).\
                    reduceByKey(reduceFunc).\
                    map(lambda p: p[1])

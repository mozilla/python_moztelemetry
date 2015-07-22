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
import backports.lzma as lzma
import json as json
import numpy.random as random
import ssl

from filter_service import SDB
from histogram import Histogram
from heka_message_parser import parse_heka_message
from xml.sax import SAXParseException

if not boto.config.has_section('Boto'):
    boto.config.add_section('Boto')
boto.config.set('Boto', 'http_socket_timeout', '10')  # https://github.com/boto/boto/issues/2830

_chunk_size = 100*(2**20)
try:
    _conn = boto.connect_s3()
    _bucket_v2 = _conn.get_bucket("telemetry-published-v2", validate=False)
    _bucket_v4 = _conn.get_bucket("net-mozaws-prod-us-west-2-pipeline-data", validate=False)
except:
    pass  # Handy for testing purposes...


def get_clients_history(sc, **kwargs):
    """ Returns RDD[client_id, ping]. This API is experimental and might change entirely at any point!
    """

    fraction = kwargs.pop("fraction", 1.0)

    if fraction < 0 or fraction > 1:
        raise ValueError("Invalid fraction argument")

    if kwargs:
        raise TypeError("Unexpected **kwargs {}".format(repr(kwargs)))

    clients = [x.name for x in list(_bucket_v4.list(prefix="telemetry_sample_42/", delimiter="/"))]

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
              flatMapValues(lambda x: _read_v4(x))


def get_pings(sc, **kwargs):
    """ Returns a RDD of Telemetry submissions for a given filtering criteria.

    Depending on the value of the 'schema' argument, different filtering criteria
    are available. By default, the 'v2' schema is assumed (classic Telemetry).
    The unified Telemetry/FHR submissions are available by selecting the 'v4' schema.

    If schema == "v2" then:
    :param app: an application name, e.g.: "Firefox"
    :param channel: a channel name, e.g.: "nightly"
    :param version: the application version, e.g.: "40.0a1"
    :param build_id: a build_id or a range of build_ids, e.g.:
                     "20150601000000" or ("20150601000000", "20150610999999")
    :param submission_date: a submission date or a range of submission dates, e.g:
                            "20150601" or ("20150601", "20150610")
    :param fraction: the fraction of pings to return, set to 1.0 by default
    :param reason: submission reason, set to "saved_session" by default, e.g: "saved_session"

    If schema == "v4" then:
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
    schema = kwargs.pop("schema", "v2")
    if schema == "v2":
        return _get_pings_v2(sc, **kwargs)
    elif schema == "v4":
        return _get_pings_v4(sc, **kwargs)
    else:
        raise ValueError("Invalid schema version")


def get_pings_properties(pings, paths, only_median=False, with_processes=False):
    """
    Returns a RDD of a subset of properties of pings. Child histograms are
    automatically merged with the parent histogram.
    """
    if type(pings.first()) == str:
        pings = pings.map(lambda p: json.loads(p))

    if type(paths) == str:
        paths = [paths]

    # Use '/' as dots can appear in keyed histograms
    paths = [(path, path.split("/")) for path in paths]
    return pings.map(lambda p: _get_ping_properties(p, paths, only_median, with_processes)).filter(lambda p: p)


def get_one_ping_per_client(pings):
    """
    Returns a single ping for each client in the RDD. This operation is expensive
    as it requires data to be shuffled around. It should be run only after extracting
    a subset with get_pings_properties.
    """
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
                    reduceByKey(lambda p1, p2: p1).\
                    map(lambda p: p[1])


def _get_client_history(client_prefix):
    try:
        return [x.name for x in list(_bucket_v4.list(prefix=client_prefix))]
    except SAXParseException:  # https://groups.google.com/forum/#!topic/boto-users/XCtTFzvtKRs
        return None


def _get_pings_v2(sc, **kwargs):
    app = kwargs.pop("app", None)
    channel = kwargs.pop("channel", None)
    version = kwargs.pop("version", None)
    build_id = kwargs.pop("build_id", None)
    submission_date = kwargs.pop("submission_date", None)
    fraction = kwargs.pop("fraction", 1.0)
    reason = kwargs.pop("reason", "saved_session")

    if fraction < 0 or fraction > 1:
        raise ValueError("Invalid fraction argument")

    if kwargs:
        raise TypeError("Unexpected **kwargs {}".format(repr(kwargs)))

    files = _get_filenames_v2(app=app, channel=channel, version=version, build_id=build_id,
                              submission_date=submission_date, reason=reason)

    if files and fraction != 1.0:
        sample = random.choice(files, size=len(files)*fraction, replace=False)
    else:
        sample = files

    parallelism = max(len(sample), sc.defaultParallelism)
    return sc.parallelize(sample, parallelism).flatMap(lambda x: _read_v2(x))


def _get_pings_v4(sc, **kwargs):
    app = kwargs.pop("app", None)
    channel = kwargs.pop("channel", None)
    version = kwargs.pop("version", None)
    build_id = kwargs.pop("build_id", None)
    submission_date = kwargs.pop("submission_date", None)
    source_name = kwargs.pop("source_name", "telemetry")
    source_version = kwargs.pop("source_version", "4")
    doc_type = kwargs.pop("doc_type", "saved_session")
    fraction = kwargs.pop("fraction", 1.0)

    if fraction < 0 or fraction > 1:
        raise ValueError("Invalid fraction argument")

    if kwargs:
        raise TypeError("Unexpected **kwargs {}".format(repr(kwargs)))

    files = _get_filenames_v4(app=app, channel=channel, version=version, build_id=build_id, submission_date=submission_date,
                              source_name=source_name, source_version=source_version, doc_type=doc_type)

    if files and fraction != 1.0:
        sample = random.choice(files, size=len(files)*fraction, replace=False)
    else:
        sample = files

    parallelism = max(len(sample), sc.defaultParallelism)
    ranges = sc.parallelize(sample, parallelism).flatMap(_read_v4_ranges).collect()

    if len(ranges) == 0:
        return sc.parallelize([])
    else:
        return sc.parallelize(ranges, len(ranges)).flatMap(_read_v4_range)


def _get_filenames_v2(**kwargs):
    translate = {"app": "appName",
                 "channel": "appUpdateChannel",
                 "version": "appVersion",
                 "build_id": "appBuildID",
                 "submission_date": "submissionDate",
                 "reason": "reason"}
    query = {}
    for k, v in kwargs.iteritems():
        tk = translate.get(k, None)
        if not tk:
            raise ValueError("Invalid query attribute name specified: {}".format(k))
        query[tk] = v

    sdb = SDB("telemetry_v2")
    return sdb.query(**query)


def _get_filenames_v4(**kwargs):
    translate = {"app": "appName",
                 "channel": "appUpdateChannel",
                 "version": "appVersion",
                 "build_id": "appBuildId",
                 "submission_date": "submissionDate",
                 "source_name": "sourceName",
                 "source_version": "sourceVersion",
                 "doc_type": "docType"}
    query = {}
    for k, v in kwargs.iteritems():
        tk = translate.get(k, None)
        if not tk:
            raise ValueError("Invalid query attribute name specified: {}".format(k))
        query[tk] = v

    sdb = SDB("telemetry_v4")
    return sdb.query(**query)


def _read_v2(filename):
    try:
        key = _bucket_v2.get_key(filename)
        compressed = key.get_contents_as_string()
        raw = lzma.decompress(compressed).split("\n")[:-1]
        return map(lambda x: x.split("\t", 1)[1], raw)
    except ssl.SSLError:
        return []


def _read_v4(filename):
    try:
        key = _bucket_v4.get_key(filename)
        key.open_read()
        return parse_heka_message(key)
    except ssl.SSLError:
        return []


def _read_v4_ranges(filename):
    try:
        key = _bucket_v4.get_key(filename)
        n_chunks = (key.size / _chunk_size) + 1
        return zip([filename]*n_chunks, range(n_chunks))
    except ssl.SSLError:
        return []


def _read_v4_range(filename_chunk):
    try:
        filename, chunk = filename_chunk
        start = _chunk_size*chunk
        key = _bucket_v4.get_key(filename)
        key.open_read(headers={'Range': "bytes={}-".format(start)})
        return parse_heka_message(key, boundary_bytes=_chunk_size)
    except ssl.SSLError:
        return []


def _get_ping_properties(ping, paths, only_median, with_processes):
    result = {}

    for property_name, path in paths:
        cursor = ping

        if path[0] == "payload":
            path = path[1:]  # Translate v4 histogram queries to v2 ones
            cursor = ping["payload"]

        if path[0] == "histograms" or path[0] == "keyedHistograms":
            props = _get_merged_histograms(cursor, property_name, path, with_processes)

            for k, v in props.iteritems():
                result[k] = v.get_value(only_median) if v else None
        else:
            prop = _get_ping_property(cursor, path)
            result[property_name] = prop

    return result


def _get_ping_property(cursor, path):
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
        return Histogram(path[-1], cursor)
    elif is_keyed_histogram:
        histogram = Histogram(path[-2], cursor)
        histogram.name = "/".join(path[-2:])
        return histogram
    else:
        return cursor


def _get_merged_histograms(cursor, property_name, path, with_processes):
    if path[0] == "histograms" and len(path) != 2:
        raise ValueError("Histogram access requires a histogram name.")
    elif path[0] == "keyedHistograms" and len(path) != 3:
        raise ValueError("Keyed histogram access requires both a histogram name and a label.")

    # Get parent histogram
    parent = _get_ping_property(cursor, path)

    # Get children histograms
    cursor = cursor.get("childPayloads", {}) if type(cursor) == dict else {}
    children = filter(lambda h: h is not None, [_get_ping_property(child, path) for child in cursor]) if cursor else []

    # Merge parent and children
    merged = ([parent] if parent else []) + children

    result = {}
    if with_processes:
        result[property_name + "_parent"] = parent
        result[property_name + "_children"] = reduce(lambda x, y: x + y, children) if children else None
    result[property_name] = reduce(lambda x, y: x + y, merged) if merged else None

    return result

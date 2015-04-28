#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

""" This module implements the Telemetry API for Spark.

Example usage:
rdd = get_pings(None, app="Firefox", channel="nightly", build_id=("20140401000000", "20140402999999"), reason="saved_session")

"""

import requests
import boto
import liblzma as lzma
import json as json
import numpy.random as random

from filter_service import  SDB
from histogram import Histogram
from heka_message_parser import parse_heka_message

_conn = boto.connect_s3()
_bucket_v2 = _conn.get_bucket("telemetry-published-v2", validate=False)
_bucket_v4 = _conn.get_bucket("net-mozaws-prod-us-west-2-pipeline-data", validate=False)


def get_pings(sc, **kwargs):
    """ Returns a RDD of Telemetry submissions for the given criteria. """
    schema = kwargs.pop("schema", "v2")
    if schema == "v2":
        return _get_pings_v2(sc, **kwargs)
    elif schema == "v4":
        return _get_pings_v4(sc, **kwargs)
    else:
        raise ValueError("Invalid schema version")


def get_pings_properties(pings, keys, only_median=False):
    """
    Returns a RDD of a subset of properties of pings. Child histograms are
    automatically merged with the parent histogram.
    """
    if type(pings.first()) == str:
        pings = pings.map(lambda p: json.loads(p))

    if type(keys) == str:
        keys = [keys]

    # Use '/' as dots can appear in keyed histograms
    keys = [key.split("/") for key in keys]
    return pings.map(lambda p: _get_ping_properties(p, keys, only_median)).filter(lambda p: p)


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

    files = _get_filenames_v2(app=app, channel=channel, version=version, build_id = build_id,
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
    submission_date = kwargs.pop("submission_date", None)
    source_name = kwargs.pop("source_name", "telemetry")
    source_version = kwargs.pop("source_version", "4")
    doc_type = kwargs.pop("doc_type", "main")
    fraction = kwargs.pop("fraction", 1.0)

    if fraction < 0 or fraction > 1:
        raise ValueError("Invalid fraction argument")

    if kwargs:
        raise TypeError("Unexpected **kwargs {}".format(repr(kwargs)))

    files = _get_filenames_v4(app=app, channel=channel, version=version, submission_date=submission_date,
                              source_name=source_name, source_version=source_version, doc_type=doc_type)

    if files and fraction != 1.0:
        sample = random.choice(files, size=len(files)*fraction, replace=False)
    else:
        sample = files

    parallelism = max(len(sample), sc.defaultParallelism)
    return sc.parallelize(sample, parallelism).flatMap(lambda x: _read_v4(x))


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
    key = _bucket_v2.get_key(filename)
    compressed = key.get_contents_as_string()
    raw = lzma.decompress(compressed).split("\n")[:-1]
    return map(lambda x: x.split("\t", 1)[1], raw)


def _read_v4(filename):
    key = _bucket_v4.get_key(filename)
    heka_message = key.get_contents_as_string()
    return list(parse_heka_message(heka_message))


def _get_ping_properties(ping, keys, only_median):
    res = {}

    for key in keys:
        if key[0] == "histograms" or key[0] == "keyedHistograms":
            props = _get_merged_histograms(ping, key)

            for k, v in props.iteritems():
                res[k] = v.get_value(only_median)
        else:
            k, v = _get_ping_property(ping, key)

            if v is None:
                continue

            res[k] = v

    return res


def _get_ping_property(cursor, key):
    is_histogram = False
    is_keyed_histogram = False

    if len(key) == 2 and key[0] == "histograms":
        is_histogram = True
    elif len(key) == 3 and key[0] == "keyedHistograms":
        is_keyed_histogram = True

    for partial in key:
        cursor = cursor.get(partial, None)

        if cursor is None:
            break

    if cursor is None:
        return (None, None)
    if is_histogram:
        return (key[-1], Histogram(key[-1], cursor))
    elif is_keyed_histogram:
        return ("/".join(key[-2:]), Histogram(key[-2], cursor))
    else:
        return (key[-1], cursor)


def _get_merged_histograms(cursor, key):
    assert((len(key) == 2 and key[0] == "histograms") or (len(key) == 3 and key[0] == "keyedHistograms"))
    res = {}

    # Get parent histogram
    name, parent = _get_ping_property(cursor, key)

    cursor = cursor.get("childPayloads", {})
    if not cursor: # pre e10s ping
        return {name: parent} if parent else {}

    if parent:
        res[name + "_parent"] = parent

    # Get children histograms
    children = filter(lambda c: c[0] is not None, [_get_ping_property(child, key) for child in cursor])

    if children:
        name = children[0][0] # The parent histogram might not exist
        children = map(lambda c: c[1], children)
        res[name + "_children"] = reduce(lambda x, y: x + y, children)

    # Merge parent and children
    if parent or children:
        metrics = ([parent] if parent else []) + children
        res[name] = reduce(lambda x, y: x + y, metrics)

    return res
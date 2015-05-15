import itertools
import binascii
import pandas as pd

from spark import get_pings, get_one_ping_per_client
from collections import OrderedDict
from histogram import cached_exponential_buckets


_exponential_index = cached_exponential_buckets(1, 30000, 50)


def aggregate_metrics(sc, channel, submission_date):
    pings = get_pings(sc, channel=channel, submission_date=submission_date, doc_type="saved_session", schema="v4", fraction=0.01)
    #pings = sc.parallelize([pings.first()])

    trimmed = get_one_ping_per_client(pings.filter(_sample_clients).map(_trim_ping))
    raw_aggregate = trimmed.flatMap(_extract_metrics).reduceByKey(_aggregate)

    frame = pd.DataFrame(raw_aggregate.map(_prettify).collect())
    frame.to_json("{}_{}.json".format(channel, submission_date))
    return frame


def _sample_clients(ping):
    client_id = ping.get("clientId", None)
    channel = ping["environment"]["settings"]["update"]["channel"]
    percentage = {"nightly": 100,
                  "aurora": 100}
    return client_id and ((binascii.crc32(client_id) % 100) < percentage[channel])


def _trim_ping(ping):
    payload = {k: v for k, v in ping["payload"].iteritems() if k in ["histograms", "keyedHistograms"]}
    return {"clientId": ping["clientId"],
            "environment": ping["environment"],
            "payload": payload}


def _extract_metrics(ping):
    dimensions = OrderedDict()
    dimensions["channel"] = ping["environment"]["settings"]["update"]["channel"]
    dimensions["version"] = ping["environment"]["build"]["platformVersion"].split('.')[0]
    dimensions["build_id"] = ping["environment"]["build"]["buildId"][:8]
    dimensions["application"] = ping["environment"]["build"]["applicationName"]
    dimensions["architecture"] = ping["environment"]["build"]["architecture"]
    dimensions["child"] = False
    dimensions["os"] = ping["environment"]["system"]["os"]["name"]
    dimensions["os_version"] = ping["environment"]["system"]["os"]["version"]
    if dimensions["os"] == "Linux":
        dimensions["os_version"] = str(dimensions["os_version"])[:3]

    top_histograms = _extract_histograms(dimensions, ping["payload"])
    dimensions["child"] = True
    child_histograms = _extract_children_histograms(dimensions, ping["payload"])
    simple_measures = _extract_simple_measures(dimensions, ping["payload"].get("simpleMeasurements", {}))
    return list(itertools.chain(top_histograms, child_histograms, simple_measures))


def _extract_histograms(dimensions, payload):
    histograms = payload["histograms"]
    for metric in _extract_main_histograms(dimensions, histograms):
        yield metric

    keyed_histograms = payload.get("keyedHistograms", {})
    for name, histograms in keyed_histograms.iteritems():
        for metric in _extract_keyed_histograms(dimensions, name, histograms):
            yield metric


def _extract_main_histograms(dimensions, histograms):
    for histogram_name, histogram in histograms.iteritems():
        histogram = pd.Series({int(k): v for k, v in histogram["values"].items()})
        yield _dimension_mapper(dimensions, histogram, histogram_name)



def _extract_keyed_histograms(dimensions, histogram_name, histograms):
    for key, histogram in histograms.iteritems():
        histogram = pd.Series({int(k): v for k, v in histogram["values"].items()})
        yield _dimension_mapper(dimensions, histogram, histogram_name, key)


def _extract_children_histograms(dimensions, payload):
    child_payloads = payload.get("childPayloads", {})
    for child in child_payloads:
        for metric in _extract_histograms(dimensions, child):
            yield metric


def _extract_simple_measures(dimensions, simple):
    for name, value in simple.iteritems():
        if type(value) == dict:
            for sub_name, sub_value in value.iteritems():
                if type(sub_value) in (int, float, long):
                    yield _extract_simple_measure(dimensions, "{}_{}".format(name, sub_name), sub_value)
        elif type(value) in (int, float, long):
            yield _extract_simple_measure(dimensions, name, value)


def _extract_simple_measure(dimensions, name, value):
    for bucket in reversed(_exponential_index):
        if value >= bucket:
            histogram = pd.Series({bucket: 1})
            break

    return _dimension_mapper(dimensions, histogram, name)


def _dimension_mapper(dimensions, histogram, metric, label=u""):
    payload = {"histogram": histogram, "count": 1, "metric": metric, "label": label}
    return tuple(dimensions.values() + [metric + label]), payload


def _aggregate(x, y):
    x["histogram"] = x["histogram"].add(y["histogram"], fill_value=0)
    x["count"] += y["count"]
    return x


def _prettify(raw_aggregate):
    key, value = raw_aggregate
    channel, version, build_id, application, architecture, child, os, os_version, metric = key
    return {"channel": channel,
            "version": version,
            "build_id": build_id,
            "application": application,
            "architecture": architecture,
            "child": child,
            "os": os,
            "os_version": os_version,
            "metric": value["metric"],
            "count": value["count"],
            "histogram": value["histogram"],
            "label": value["label"]}


if __name__ == "__main__":
    from pyspark import SparkContext
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

    # Set SPARK_HOME to Spark installation directory
    # Set PYTHONPATH to $SPARK_HOME/libexec/python
    parser = ArgumentParser(description="Telemetry histogram aggregation utility",
                            formatter_class=ArgumentDefaultsHelpFormatter)


    parser.add_argument("-c", "--channel", help="Submission channel", default="nightly")
    parser.add_argument("-d", "--submission_date", help="Submission date")

    args = parser.parse_args()
    sc = SparkContext("local[*]", "Aggregator Test")
    aggregate_metrics(sc, args.channel, args.submission_date)

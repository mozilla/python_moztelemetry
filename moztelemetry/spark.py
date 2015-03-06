import requests
import boto
import liblzma as lzma
import simplejson as json
import histogram

_conn = boto.connect_s3()
_bucket = _conn.get_bucket("telemetry-published-v2", validate=False)

def get_pings(sc, appName, channel, version, buildid, submission_date, fraction=1.0, reason="saved-session"):
    filter = _build_filter(appName, channel, version, buildid, submission_date, reason)
    files = _get_filenames(filter)
    sample = files[len(files) - int(len(files)*fraction):]
    parallelism = max(len(sample), sc.defaultParallelism)

    return sc.parallelize(sample, parallelism).flatMap(lambda x: _read(x))

def get_pings_properties(pings, keys, only_median=False):
    if type(pings.first()) == str:
        pings = pings.map(lambda p: json.loads(p))

    if type(keys) == str:
        keys = [keys]

    keys = [key.split("/") for key in keys]
    return pings.map(lambda p: _get_ping_properties(p, keys, only_median)).filter(lambda p: p)

def filter_independent_pings(pings):
    if type(pings.first()) == str:
        pings = pings.map(lambda p: json.loads(p))

    return pings.groupBy(lambda p: p.get("clientID", None)).map(lambda x: next(iter(x[1])))

def _build_filter(appName, channel, version, buildid, submission_date, reason):
    def parse(field):
        if isinstance(field, tuple):
            return {"min": field[0], "max": field[1]}
        else:
            return field

    filter = { "filter":
               {
                   "version": 1,
                   "dimensions": [
                       {
                           "field_name": "reason",
                           "allowed_values": [reason]
                       },
                       {
                           "field_name": "appName",
                           "allowed_values": [appName]
                       },
                       {
                           "field_name": "appUpdateChannel",
                           "allowed_values": [channel]
                       },
                       {
                           "field_name": "appVersion",
                           "allowed_values": parse(version)
                       },
                       {
                           "field_name": "appBuildID",
                           "allowed_values": parse(buildid)
                       },
                       {
                           "field_name": "submission_date",
                           "allowed_values": parse(submission_date)
                       }
                   ]
               }
             }
    return json.dumps(filter)

def _get_filenames(filter):
    url = "http://ec2-54-203-209-235.us-west-2.compute.amazonaws.com:8080/files"
    headers = {"content-type": "application/json"}
    response = requests.post(url, data=filter, headers=headers)

    try:
        return response.json()["files"]
    except:
        return []

def _read(filename):
    key = _bucket.get_key(filename)
    compressed = key.get_contents_as_string()
    raw = lzma.decompress(compressed).split("\n")[:-1]
    return map(lambda x: x.split("\t", 1)[1], raw)

def _get_ping_property(cursor, key, only_median=False):
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
        return (key[-1], Histogram(key[-1], cursor).get_value(only_median))
    elif is_keyed_histogram:
        return ("/".join(key[-2:]), Histogram(key[-2], cursor).get_value(only_median))
    else:
        return (key[-1], cursor)

def _get_ping_properties(ping, keys, only_median=False):
    res = {}

    for key in keys:
        k, v = _get_ping_property(ping, key, only_median)
        if k:
            res[k] = v

    return res

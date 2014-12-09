import requests
import boto
import liblzma as lzma
import simplejson as json

def _build_filter(appName, channel, version, buildid, submission_date):
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
                           "allowed_values": ["saved-session"]
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
    conn = boto.connect_s3()
    bucket = conn.get_bucket("telemetry-published-v2", validate=False)
    key = bucket.get_key(filename)
    compressed = key.get_contents_as_string()
    raw = lzma.decompress(compressed).split("\n")[:-1]
    return map(lambda x: x[37:], raw)

def get_pings(sc, appName, channel, version, buildid, submission_date, fraction=1.0):
    filter = _build_filter(appName, channel, version, buildid, submission_date)
    files = _get_filenames(filter)
    sample = files[0: int(len(files)*fraction)]
    parallelism = max(len(sample) / 16, sc.defaultParallelism)

    return sc.parallelize(sample, parallelism).flatMap(lambda x: _read(x))
    read(files)

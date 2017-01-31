# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
from moztelemetry.heka import message_parser
from moztelemetry.util.streaming_gzip import streaming_gzip_wrapper


def test_unpack(data_dir):
    for t in ["plain", "snappy", "mixed", "gzip", "gzip_mixed"]:
        filename = "{}/test_{}.heka".format(data_dir, t)
        with open(filename, "rb") as o:
            if "gzip" in t:
                o = streaming_gzip_wrapper(o)
            msg = 0
            for r, b in message_parser.unpack(o, try_snappy=True):
                j = json.loads(r.message.payload)
                assert msg == j["seq"]
                msg += 1
            assert 10 == msg


def test_unpack_nosnappy(data_dir):
    expected_counts = {"plain": 10, "snappy": 0, "mixed": 5,
                       "gzip": 10, "gzip_mixed": 5}
    for t in expected_counts.keys():
        count = 0
        filename = "{}/test_{}.heka".format(data_dir, t)
        with open(filename, "rb") as o:
            if "gzip" in t:
                o = streaming_gzip_wrapper(o)
            try:
                for r, b in message_parser.unpack(o, try_snappy=False):
                    count += 1
            except:
                pass
            assert expected_counts[t] == count


def test_unpack_strict(data_dir):
    expected_exceptions = {"plain": False, "snappy": True, "mixed": True,
                           "gzip": False, "gzip_mixed": True}
    for t in expected_exceptions.keys():
        count = 0
        filename = "{}/test_{}.heka".format(data_dir, t)
        threw = False
        got_err = False
        with open(filename, "rb") as o:
            if "gzip" in t:
                o = streaming_gzip_wrapper(o)
            try:
                for r, b in message_parser.unpack(o, strict=True, try_snappy=False):
                    if r.error is not None:
                        got_err = True
                    count += 1
            except Exception as e:
                threw = True
        assert expected_exceptions[t] == threw

top_keys = set(["application", "clientId", "creationDate", "environment", "id", "meta",
                "payload", "type", "version"])
payload_keys = set(["UIMeasurements", "addonDetails", "childPayloads", "chromeHangs",
                    "fileIOReports", "histograms", "info", "keyedHistograms", "lateWrites",
                    "log", "processes", "simpleMeasurements", "slowSQL", "threadHangStats",
                    "ver", "webrtc"])
def test_telemetry(data_dir):
    filename = "{}/test_telemetry_gzip.heka".format(data_dir)
    with open(filename, "rb") as o:
        for r in message_parser.parse_heka_message(streaming_gzip_wrapper(o)):
            assert set(r.keys()) == top_keys
            assert set(r["payload"].keys()) == payload_keys

    filename = "{}/test_telemetry_snappy.heka".format(data_dir)
    with open(filename, "rb") as o:
        for r in message_parser.parse_heka_message(o):
            assert set(r.keys()) == top_keys
            assert set(r["payload"].keys()) == payload_keys

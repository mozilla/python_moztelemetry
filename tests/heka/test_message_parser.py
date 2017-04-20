# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import copy
import json
import pytest
from google.protobuf.message import DecodeError
from mock import MagicMock
from moztelemetry.heka import message_parser
from moztelemetry.util.streaming_gzip import streaming_gzip_wrapper


@pytest.mark.parametrize("heka_format,try_snappy,strict,expected_count,expected_exception", [
    # snappy disabled
    ("plain", False, False, 10, False),
    ("snappy", False, False, 0, False),
    ("mixed", False, False, 5, False),
    ("gzip",  False, False, 10, False),
    ("gzip_mixed", False, False, 5, False),
    # snappy enabled
    ("plain", True, False, 10, False),
    ("snappy", True, False, 10, False),
    ("mixed", True, False, 10, False),
    ("gzip",  True, False, 10, False),
    ("gzip_mixed", True, False, 10, False),
    # strict mode on
    ("plain", False, True, 10, False),
    ("snappy", False, True, 0, True),
    ("mixed", False, True, 5, True),
    ("gzip",  False, True, 10, False),
    ("gzip_mixed", False, True, 5, True)
])
def test_unpack(data_dir, heka_format, try_snappy, strict, expected_count,
                expected_exception):
    count = 0
    threw_exception = False
    filename = "{}/test_{}.heka".format(data_dir, heka_format)
    with open(filename, "rb") as o:
        if "gzip" in heka_format:
            o = streaming_gzip_wrapper(o)
        try:
            for r, b in message_parser.unpack(o, try_snappy=try_snappy, strict=strict):
                j = json.loads(r.message.payload)
                assert count == j["seq"]
                count += 1
        except DecodeError:
            threw_exception = True

    assert count == expected_count
    assert threw_exception == expected_exception


top_keys = set(["application", "clientId", "creationDate", "environment", "id", "meta",
               "payload", "type", "version"])
payload_keys = set(["UIMeasurements", "addonDetails", "childPayloads", "chromeHangs",
                    "fileIOReports", "histograms", "info", "keyedHistograms", "lateWrites",
                    "log", "processes", "simpleMeasurements", "slowSQL", "threadHangStats",
                    "ver", "webrtc"])
@pytest.mark.parametrize("heka_format", ["snappy", "gzip"])
def test_parse_heka_message(data_dir, heka_format):
    filename = "{}/test_telemetry_{}.heka".format(data_dir, heka_format)
    with open(filename, "rb") as o:
        for r in message_parser.parse_heka_message(streaming_gzip_wrapper(o)):
            assert set(r.keys()) == top_keys
            assert set(r["payload"].keys()) == payload_keys


def test_invalid_utf8(data_dir):
    filename = "{}/test_invalid_utf8.heka".format(data_dir)
    with open(filename, "rb") as o:
        for r in message_parser.parse_heka_message(o):
            assert(u'\ufffd' in r['info']['adapterDescription'])

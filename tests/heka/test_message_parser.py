# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
from moztelemetry.heka import message_parser


def test_unpack(data_dir):
    for t in ["plain", "snappy", "mixed"]:
        filename = "{}/test_{}.heka".format(data_dir, t)
        with open(filename, "rb") as o:
            msg = 0
            for r, b in message_parser.unpack(o, try_snappy=True):
                j = json.loads(r.message.payload)
                assert msg == j["seq"]
                msg += 1
            assert 10 == msg


def test_unpack_nosnappy(data_dir):
    expected_counts = {"plain": 10, "snappy": 0, "mixed": 5}
    for t in expected_counts.keys():
        count = 0
        filename = "{}/test_{}.heka".format(data_dir, t)
        with open(filename, "rb") as o:
            try:
                for r, b in message_parser.unpack(o, try_snappy=False):
                    count += 1
            except:
                pass
            assert expected_counts[t] == count


def test_unpack_strict(data_dir):
    expected_exceptions = {"plain": False, "snappy": True, "mixed": True}
    for t in expected_exceptions.keys():
        count = 0
        filename = "{}/test_{}.heka".format(data_dir, t)
        threw = False
        got_err = False
        with open(filename, "rb") as o:
            try:
                for r, b in message_parser.unpack(o, strict=True, try_snappy=False):
                    if r.error is not None:
                        got_err = True
                    count += 1
            except Exception as e:
                threw = True
        assert expected_exceptions[t] == threw

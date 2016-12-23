# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
import happybase
import uuid

from datetime import date, timedelta
from moztelemetry.hbase import HBaseMainSummaryView
from py4j.protocol import Py4JJavaError

_client_id = str(uuid.uuid4())
_num_pings = 10


@pytest.fixture
def view():
    conn = happybase.Connection("localhost")
    table_name = "main_summary"
    column_family = "cf"
    column = "{}:payload".format(column_family)

    if table_name in conn.tables():
        conn.disable_table(table_name)
        conn.delete_table(table_name)

    conn.create_table(table_name, {column_family: dict()})
    table = conn.table(table_name)

    for i in range(0, _num_pings):
        d = date(2016, 12, 1) + timedelta(days=i)
        key = "{}:{}:{}".format(_client_id, d.strftime("%Y%m%d"), uuid.uuid4())
        table.put(key, {column: "{}"})

    view = HBaseMainSummaryView("localhost")
    return view


def test_get(spark_context, view):
    histories = view.get(spark_context, [_client_id]).collect()
    assert len(histories) == 1
    assert histories[0][0] == _client_id
    assert len(histories[0][1]) == _num_pings

    with pytest.raises(TypeError):
        view.get(spark_context, _client_id)

    with pytest.raises(TypeError):
        next(view.get(spark_context, [_client_id], limit=3.0))

    with pytest.raises(Py4JJavaError):
        view.get(spark_context, ["foo"]).collect()


def test_get_range(spark_context, view):
    histories = view.get_range(spark_context, [_client_id], date(2016, 12, 1), date(2016, 12, 3)).collect()
    assert len(histories) == 1
    assert histories[0][0] == _client_id
    assert len(histories[0][1]) == 3

    with pytest.raises(Py4JJavaError):
        view.get_range(spark_context, ["foo"], date(2016, 12, 1), date(2016, 12, 3)).collect()

    with pytest.raises(TypeError):
        view.get_range(spark_context, [_client_id], "20161201", "20161203")

    with pytest.raises(TypeError):
        view.get_range(spark_context, [_client_id], date(2016, 12, 1), date(2016, 12, 3), limit=3.0)

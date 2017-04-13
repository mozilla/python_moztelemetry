# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

import happybase
import ujson as json
import uuid
import boto3
import contextlib

from datetime import date, timedelta
from functools import partial


class HBaseMainSummaryView:
    """ The access gateway to the HBase main summary view

    The gateway allows to retrieve the history of pings for a *small* set of client ids.
    The retrieval can optionally be limited to a time period of activity for said clients.

    Usage example::

        view = HBaseMainSummaryView()

        for client_id, pings in view.get(sc, ["00000000-0000-0000-0000-000000000000"], limit=10).collect():
                print client_id
                for ping in pings:
                    print ping["subsession_start_date"]

        for client_id, pings in view.get_range(sc, ["00000000-0000-0000-0000-000000000000"],
                range_start=date(2016, 12, 1), range_end=date(2016, 12, 2)).collect():
                print client_id
                for ping in pings:
                    print ping["subsession_start_date"]

    Note that retrieving the whole ping is not only slower, but also not needed for most analyses.
    Please try to collect back to the driver only the data you really need.

    Fast retrieval example::

        for client_id, pings in view.get(sc, ["00000000-0000-0000-0000-000000000000"], limit=10) \
                                    .map(lambda kv: (kv[0], [p["subsession_start_date"] for p in kv[1]])) \
                                    .collect():
            print client_id
            for ping in pings:
                print ping

    """

    def __init__(self, hostname=None):
        self.tablename = 'main_summary'
        self.column_family = 'cf'
        self.column = 'cf:payload'

        if hostname is None:
            try:
                self.hostname = self._get_master_address()
            except:
                raise Exception("Failure to retrieve HBase address")
        else:
            self.hostname = hostname

    def _get_master_address(self):
        client = boto3.client('ec2')
        reservations = client.describe_instances(
            Filters=[{'Name': 'tag:Name',
                      'Values': ['telemetry-hbase']},
                     {'Name': 'tag:aws:elasticmapreduce:instance-group-role',
                      'Values': ['MASTER']}])["Reservations"]

        if len(reservations) == 0:
            raise Exception("HBase master not found!")

        if len(reservations) > 1:
            raise Exception("Multiple HBase masters found!")

        return reservations[0]["Instances"][0]["NetworkInterfaces"][0]["PrivateIpAddress"]

    def _validate_client_id(self, client_id):
        try:
            uuid.UUID(client_id)
            return True
        except ValueError:
            return False

    def get(self, sc, client_ids, limit=None, parallelism=None, reverse=False):
        """ Return RDD[client_id, [ping1, ..., pingK]]

        The list of pings is sorted by activity date.

        :param sc: a SparkContext
        :param client_ids: the client ids represented as UUIDs
        :param limit: the maximum number of pings to return per client id
        :param parallelism: the number of partitions of the resulting RDD
        :param reverse: whether to return pings in reverse chronological order, defaults to False
        """
        if not isinstance(client_ids, (list, tuple, )):
            raise TypeError('client_ids must be a list or a tuple'.format(type(client_ids)))

        if not isinstance(limit, (int, type(None))):
            raise TypeError('limit must be either an int or None, not {}'.format(type(limit)))

        if parallelism is None:
            parallelism = sc.defaultParallelism

        def _get(client_id, limit):
            if not self._validate_client_id(client_id):
                raise ValueError("Invalid Client ID!")

            payloads = []
            with contextlib.closing(happybase.Connection(self.hostname)) as connection:
                table = connection.table(self.tablename)
                row_start = "{}:{}".format(client_id, "99999999" if reverse else "")

                for key, data in table.scan(row_start=row_start, limit=limit,
                                            columns=[self.column_family], reverse=reverse):
                    payloads.append(json.loads(data[self.column]))

            return (client_id, payloads)

        return sc.parallelize(client_ids, parallelism)\
            .map(partial(_get, limit=limit))

    def get_range(self, sc, client_ids, range_start, range_end, limit=None, parallelism=None, reverse=False):
        """ Return RDD[client_id, [ping1, ..., pingK]] where pings are limited
        to a given activity period.

        The list of pings is sorted by activity date.

        :param sc: a SparkContext
        :param client_ids: the client ids represented as UUIDs
        :param range_start: the beginning of the time period represented as a datetime.date instance
        :param range_end: the end of the time period (inclusive) represented as a datetime.date instance
        :param limit: the maximum number of pings to return per client id
        :param parallelism: the number of partitions of the resulting RDD
        :param reverse: whether to return pings in reverse chronological order, defaults to False
        """
        if not isinstance(range_start, date):
            raise TypeError('range_start must be a datetime.date, not {}'.format(type(range_start)))

        if not isinstance(range_end, date):
            raise TypeError('range_end must be a datetime.date, not {}'.format(type(range_end)))

        if not isinstance(limit, (int, type(None))):
            raise TypeError('limit must be either an int or None, not {}'.format(type(limit)))

        range_start = range_start.strftime("%Y%m%d")
        range_end = (range_end + timedelta(days=1)).strftime("%Y%m%d")

        if parallelism is None:
            parallelism = sc.defaultParallelism

        def _get_range(client_id, range_start, range_end, limit):
            if not self._validate_client_id(client_id):
                raise ValueError("Invalid Client ID!")

            row_start = "{}:{}".format(client_id, range_start)
            row_stop = "{}:{}".format(client_id, range_end)

            if reverse:
                row_start, row_stop = row_stop, row_start

            payloads = []
            with contextlib.closing(happybase.Connection(self.hostname)) as connection:
                table = connection.table(self.tablename)
                for key, data in table.scan(row_start=row_start, row_stop=row_stop, limit=limit,
                                            columns=[self.column_family], reverse=reverse):
                    payloads.append(json.loads(data[self.column]))

            return (client_id, payloads)

        return sc.parallelize(client_ids, parallelism)\
            .map(partial(_get_range, range_start=range_start, range_end=range_end, limit=limit))

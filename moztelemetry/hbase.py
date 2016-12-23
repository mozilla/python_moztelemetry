import happybase
import ujson as json
import uuid
import time
import boto3

from datetime import date, timedelta

__all__ = ['HBaseMainSummaryView']


class HBaseTable:
    def __init__(self, hostname, tablename):
        self.hostname = hostname
        self.tablename = tablename

    def __enter__(self):
        self.connection = happybase.Connection(self.hostname)
        return self.connection.table(self.tablename)

    def __exit__(self, *args):
        self.connection.close()


class HBaseMainSummaryView:
    """ The access gateway to the HBase main summary view

    The gateway allows to retrieve the history of pings for a specified client id.
    The retrieval can optionally be limited to a time period of activity for said client.

    Usage example::


    for ping in view.get("00000000-0000-0000-0000-000000000000", limit=10):
        print ping["subsession_start_date"]

    for ping in view.get_range("00000000-0000-0000-0000-000000000000",
                               range_start=date(2016, 12, 1),
                               range_end=date(2016, 12, 2)):
        print ping["subsession_start_date"]
    """
    def __init__(self):
        client = boto3.client('ec2')
        hbase_elastic_ip = "52.24.192.75"
        self.tablename = 'main_summary'
        self.column_family = 'cf'
        self.column = 'cf:payload'
        self.hostname = client.describe_addresses(PublicIps=[hbase_elastic_ip])["Addresses"][0]["PrivateIpAddress"]

    def _validate_client_id(self, client_id):
        try:
            uuid.UUID(client_id)
            return True
        except:
            return False

    """ Return an iterator over the client's history.

    Pings are returned in ascending order sorted by activity day.

    :param client_id: the client id represented as an UUID
    :param limit: the maximum number of pings to return
    """
    def get(self, client_id, limit=None):
        if not self._validate_client_id(client_id):
            raise ValueError("Invalid Client ID!")

        if not isinstance(limit, (int, type(None))):
            raise TypeError('limit must be either an int or None, not {}'.format(type(limit)))

        with HBaseTable(self.hostname, self.tablename) as table:
            for key, data in table.scan(row_prefix=client_id, limit=limit, columns=[self.column_family]):
                yield json.loads((key, data)[1][self.column])

    """ Return an iterator over the client's history limited to a time period of activity.

    Pings are returned in ascending order sorted by activity day.

    :param client_id: the client id represented as an UUID
    :param range_start: the beginning of the time period represented as a datetime.date instance
    :param range_end: the end of the time period (inclusive) represented as a datetime.date instance
    :param limit: the maximum number of pings to return
    """
    def get_range(self, client_id, range_start, range_end, limit=None):
        if not self._validate_client_id(client_id):
            raise ValueError("Invalid Client ID!")

        if not isinstance(range_start, date):
            raise TypeError('range_start must be a datetime.date, not {}'.format(type(range_start)))

        if not isinstance(range_end, date):
            raise TypeError('range_end must be a datetime.date, not {}'.format(type(range_end)))

        if not isinstance(limit, (int, type(None))):
            raise TypeError('limit must be either an int or None, not {}'.format(type(limit)))

        row_start = "{}:{}".format(client_id, range_start.strftime("%Y%m%d"))
        row_stop = "{}:{}".format(client_id, (range_end + timedelta(days=1)).strftime("%Y%m%d"))

        with HBaseTable(self.hostname, self.tablename) as table:
            for key, data in table.scan(row_start=row_start, row_stop=row_stop, limit=limit, columns=[self.column_family]):
                yield json.loads((key, data)[1][self.column])

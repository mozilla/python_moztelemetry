#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

""" This module implements some standard functionality based on Telemetry data.
"""

from binascii import crc32
from datetime import datetime, timedelta, date
from pyspark.sql.functions import udf

epoch = datetime.utcfromtimestamp(0)


def unix_time_nanos(dt):
    return (dt - epoch).total_seconds() * 1000000000.0


def daynum_to_date(daynum, max_days=1000000):
    """ Convert a number of days to a date. If it's out of range, default to a
    max date. If it is not a number (or a numeric string), return None. Using
    a max_days of more than 2932896 (9999-12-31) will throw an exception if the
    specified daynum exceeds the max.
    :param daynum: A number of days since Jan 1, 1970
    """
    if daynum is None:
        return None
    try:
        daycount = int(daynum)
    except ValueError:
        return None

    if daycount > max_days:
        # Using default: some time in the 48th century, clearly bogus.
        daycount = max_days
    return date(1970, 1, 1) + timedelta(daycount)


def filter_date_range(dataframe, activity_col, min_activity_incl,
                      max_activity_excl, submission_col,
                      min_submission_incl, max_submission_incl):
    return dataframe.filter(submission_col >= min_submission_incl) \
                    .filter(submission_col <= max_submission_incl) \
                    .filter(activity_col >= min_activity_incl) \
                    .filter(activity_col < max_activity_excl)


def count_distinct_clientids(dataframe):
    return dataframe.select('clientId').distinct().count()


def dau(dataframe, target_day, future_days=10, date_format="%Y%m%d"):
    """Compute Daily Active Users (DAU) from the Executive Summary dataset.
    See https://bugzilla.mozilla.org/show_bug.cgi?id=1240849
    """
    target_day_date = datetime.strptime(target_day, date_format)
    min_activity = unix_time_nanos(target_day_date)
    max_activity = unix_time_nanos(target_day_date + timedelta(1))
    act_col = dataframe.activityTimestamp

    min_submission = target_day
    max_submission_date = target_day_date + timedelta(future_days)
    max_submission = datetime.strftime(max_submission_date, date_format)
    sub_col = dataframe.submission_date_s3

    filtered = filter_date_range(dataframe, act_col, min_activity, max_activity,
                                 sub_col, min_submission, max_submission)
    return count_distinct_clientids(filtered)


def mau(dataframe, target_day, past_days=28, future_days=10, date_format="%Y%m%d"):
    """Compute Monthly Active Users (MAU) from the Executive Summary dataset.
    See https://bugzilla.mozilla.org/show_bug.cgi?id=1240849
    """
    target_day_date = datetime.strptime(target_day, date_format)

    # Compute activity over `past_days` days leading up to target_day
    min_activity_date = target_day_date - timedelta(past_days)
    min_activity = unix_time_nanos(min_activity_date)
    max_activity = unix_time_nanos(target_day_date + timedelta(1))
    act_col = dataframe.activityTimestamp

    min_submission = datetime.strftime(min_activity_date, date_format)
    max_submission_date = target_day_date + timedelta(future_days)
    max_submission = datetime.strftime(max_submission_date, date_format)
    sub_col = dataframe.submission_date_s3

    filtered = filter_date_range(dataframe, act_col, min_activity, max_activity,
                                 sub_col, min_submission, max_submission)
    return count_distinct_clientids(filtered)


def snap_to_beginning_of_week(day, weekday_start="Sunday"):
    """ Get the first day of the current week.

    :param day: The input date to snap.
    :param weekday_start: Either "Monday" or "Sunday", indicating the first day of the week.
    :returns: A date representing the first day of the current week.
    """
    delta_days = ((day.weekday() + 1) % 7) if weekday_start is "Sunday" else day.weekday()
    return day - timedelta(days=delta_days)


def snap_to_beginning_of_month(day):
    """ Get the date for the first day of this month.

    :param day: The input date to snap.
    :returns: A date representing the first day of the current month.
    """
    return day.replace(day=1)


def get_last_week_range(weekday_start="Sunday"):
    """ Gets the date for the first and the last day of the previous complete week.

    :param weekday_start: Either "Monday" or "Sunday", indicating the first day of the week.
    :returns: A tuple containing two date objects, for the first and the last day of the week
              respectively.
    """
    today = date.today()
    # Get the first day of the past complete week.
    start_of_week = snap_to_beginning_of_week(today, weekday_start) - timedelta(weeks=1)
    end_of_week = start_of_week + timedelta(days=6)
    return (start_of_week, end_of_week)


def get_last_month_range():
    """ Gets the date for the first and the last day of the previous complete month.

    :returns: A tuple containing two date objects, for the first and the last day of the month
              respectively.
    """
    today = date.today()
    # Get the last day for the previous month.
    end_of_last_month = snap_to_beginning_of_month(today) - timedelta(days=1)
    start_of_last_month = snap_to_beginning_of_month(end_of_last_month)
    return (start_of_last_month, end_of_last_month)


def read_main_summary(spark,
                      submission_date_s3=None,
                      sample_id=None,
                      mergeSchema=True,
                      path='s3://telemetry-parquet/main_summary/v4'):
    """ Efficiently read main_summary parquet data.

    Read data from the given path, optionally filtering to a
    specified set of partition values first. This can save a
    time, particularly if `mergeSchema` is True.

    Args:
        spark: Spark session
        submission_date_s3: Optional list of values to filter the
            `submission_date_s3` partition. Default is to read all
            partitions. Each value should be in the form `YYYYMMDD`.
        sample_id: Optional list of values to filter the `sample_id`
            partition. Default is to read all partitions.
        mergeSchema (bool): Determines whether or not to merge the
            schemas of the resulting parquet files (ie. whether to
            support schema evolution or not). Default is to merge
            schemas.
        path (str): Location (disk or S3) from which to read data.
            Default is to read from the "production" location on S3.

    Returns:
        A DataFrame loaded from the specified partitions.

    """
    base_path = path

    # Specifying basePath retains the partition fields even
    # if we read a bunch of paths separately.
    reader = spark.read.option("basePath", base_path)
    if mergeSchema:
        reader = reader.option("mergeSchema", "true")

    if submission_date_s3 is not None and sample_id is None:
        paths = ["{}/submission_date_s3={}/".format(base_path, s) for s in submission_date_s3]
        return reader.parquet(*paths)

    if submission_date_s3 is not None and sample_id is not None:
        paths = []
        for sd in submission_date_s3:
            for si in sample_id:
                paths.append("{}/submission_date_s3={}/sample_id={}/".format(
                    base_path, sd, si))
        return reader.parquet(*paths)

    if submission_date_s3 is None and sample_id is not None:
        # Ugh, why? We would have to iterate the entire path to identify
        # all the submission_date_s3 partitions, which may end up being
        # slower.
        data = reader.parquet(base_path)
        sids = ["{}".format(s) for s in sample_id]
        criteria = "sample_id IN ({})".format(",".join(sids))
        return data.where(criteria)

    # Neither partition is filtered.
    return reader.parquet(base_path)


def sampler(dataframe, modulo, column="client_id", sample_id=42):
    """ Collect a sample of clients given an input column

    Filter dataframe based on the modulus of the CRC32 of a given string
    column matching a given sample_id. if dataframe has already been filtered
    by sample_id, then modulo should be a multiple of 100, column should be
    "client_id", and the given sample_id should match the value previously
    used, optionally plus multiples of 100.

    Args:
        dataframe: A Dataframe to be sampled
        modulo (int): selects a 1/modulo sampling of dataframe
        column (str): name of a string column to sample on
        sample_id (int): modulus result to select for sampling

    Returns:
        A DataFrame sampled on the given inputs.
    """
    return dataframe \
        .withColumn(
            "sampler",
            udf(lambda key: (crc32(key or "") & 0xffffffff) % modulo)(column),
        ).where("sampler = %s" % sample_id).drop("sampler")

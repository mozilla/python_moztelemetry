#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

""" This module implements some standard functionality based on Telemetry data.
"""

from datetime import datetime, timedelta

epoch = datetime.utcfromtimestamp(0)

def unix_time_nanos(dt):
    return (dt - epoch).total_seconds() * 1000000000.0

def filter_date_range(dataframe, activity_col, min_activity_incl,
                      max_activity_excl, submission_col,
                      min_submission_incl, max_submission_incl):
    return dataset.filter(submission_col >= min_submission_incl)
                  .filter(submission_col <= max_submission_incl)
                  .filter(activity_col >= min_activity_incl)
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

    filtered = filter_date_range(dataset, act_col, min_activity, max_activity,
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

    filtered = filter_date_range(dataset, act_col, min_activity, max_activity,
        sub_col, min_submission, max_submission)
    return count_distinct_clientids(filtered)

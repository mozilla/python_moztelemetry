#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This module implements test coverage for the date manipulation functions in standards.py.
"""
import moztelemetry.standards as moz_utils
from datetime import date
from mock import patch

def test_snapping():
    # Use Friday, July 3rd, 2015 as the reference date.
    ref_date = date(2015, 7, 3)

    # No weekday_start argument provided, snap ref_date to the closest, previous Sunday.
    expected_date = date(2015, 6, 28) # Sunday, 28th June, 2015
    snapped_date = moz_utils.snap_to_beginning_of_week(ref_date)
    assert expected_date == snapped_date

    # Does this still work correctly when snapping to the closest Monday instead?
    expected_date = date(2015, 6, 29) # Monday, 29th June, 2015
    snapped_date = moz_utils.snap_to_beginning_of_week(ref_date, "Monday")
    assert expected_date == snapped_date

    # Check that the correct date is returned when the reference date is Sunday.
    ref_date = expected_date = date(2015, 6, 28)
    snapped_date = moz_utils.snap_to_beginning_of_week(ref_date)
    assert expected_date == snapped_date

    # Can we correctly snap to the beginning of the month?
    ref_date = date(2015, 7, 3)
    expected_date = date(2015, 7, 1)
    snapped_date = moz_utils.snap_to_beginning_of_month(ref_date)
    assert expected_date == snapped_date

    # What if we're already at the beginning of the month?
    ref_date = expected_date = date(2015, 7, 1)
    snapped_date = moz_utils.snap_to_beginning_of_month(ref_date)
    assert expected_date == snapped_date

def test_last_week_range():
    def test_week(week, startday_num, endday_num):
        # Did we get a Sunday as the beginning of the week...
        assert week[0].weekday() == startday_num
        # ... and a Saturday as the end of the week?
        assert week[1].weekday() == endday_num

        # Is this a full week spanning for exactly 7 days?
        delta = week[1] - week[0]
        assert delta.days == 6

        # Check if that's the closest full week. We monkey patched the 3rd July 2015
        # as "today". The previous full week, starting on Sunday, begins on the 21st
        # June 2015. If the first weekday is Monday, then on the 22nd of July.
        expected_dates = (date(2015, 6, 21), date(2015, 6, 27)) if\
                         startday_num == 6 else (date(2015, 6, 22), date(2015, 6, 28))

        assert week[0] == expected_dates[0]
        assert week[1] == expected_dates[1]

    with patch('moztelemetry.standards.date') as mock_date:
        # Mock date.today() to return a specific day, so we can properly test.
        mock_date.today.return_value = date(2015, 7, 3)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

        # Get the start and end date for the previous full week, as a tuple, and make sure
        # it's valid.
        prev_week = moz_utils.get_last_week_range("Sunday")
        test_week(prev_week, 6, 5)

        # As before, with a week starting with Monday.
        prev_week = moz_utils.get_last_week_range("Monday")
        test_week(prev_week, 0, 6)

def test_last_month_range():
    with patch('moztelemetry.standards.date') as mock_date:
        # Mock date.today() to return a specific day, so we can properly test.
        mock_date.today.return_value = date(2015, 7, 3)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

        # Get the start and end date for the previous full month, as a tuple, and make sure
        # it's valid.
        prev_month = moz_utils.get_last_month_range()

        assert prev_month[0] == date(2015, 6, 1)
        assert prev_month[1] == date(2015, 6, 30)

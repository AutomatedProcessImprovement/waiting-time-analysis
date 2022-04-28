import pandas as pd
import pytest
from bpdfr_simulation_engine.resource_calendar import CalendarFactory

import process_waste.calendar.intervals
from process_waste import RESOURCE_KEY, ACTIVITY_KEY, END_TIMESTAMP_KEY, START_TIMESTAMP_KEY
from process_waste.calendar import calendar
from process_waste.calendar.calendar import UNDIFFERENTIATED_RESOURCE_POOL_KEY
from process_waste.calendar.intervals import WeekDay, Interval, intersect_intervals
from process_waste.core import core


@pytest.fixture
def log_calendar(assets_path) -> dict:
    log_path = assets_path / 'PurchasingExample.csv'

    event_log = core.read_csv(log_path)
    calendar_factory = CalendarFactory(15)
    for (index, event) in event_log.iterrows():
        resource = event[RESOURCE_KEY]
        activity = event[ACTIVITY_KEY]
        start_time = event[START_TIMESTAMP_KEY]
        end_time = event[END_TIMESTAMP_KEY]
        calendar_factory.check_date_time(resource, activity, start_time)
        calendar_factory.check_date_time(resource, activity, end_time)
    calendar_candidates = calendar_factory.build_weekly_calendars(0.1, 0.7, 0.4)
    calendar = {}
    for resource_id in calendar_candidates:
        if calendar_candidates[resource_id] is not None:
            calendar[resource_id] = calendar_candidates[resource_id].to_json()
    return calendar


def test_calendar_discovery(assets_path):
    log_path = assets_path / 'PurchasingExample.csv'
    event_log = core.read_csv(log_path)
    calendar_factory = CalendarFactory(15)
    for (index, event) in event_log.iterrows():
        resource = event[RESOURCE_KEY]
        activity = event[ACTIVITY_KEY]
        start_time = event[START_TIMESTAMP_KEY]
        end_time = event[END_TIMESTAMP_KEY]
        calendar_factory.check_date_time(resource, activity, start_time)
        calendar_factory.check_date_time(resource, activity, end_time)
    calendar_candidates = calendar_factory.build_weekly_calendars(0.1, 0.7, 0.4)
    discovered_calendar = {}
    for resource_id in calendar_candidates:
        if calendar_candidates[resource_id] is not None:
            discovered_calendar[resource_id] = calendar_candidates[resource_id].to_json()

    # output_path = log_path.with_stem(log_path.stem + '_calendar').with_suffix('.json')
    # with output_path.open('w') as f:
    #     json.dump(discovered_calendar, f)

    assert discovered_calendar is not None
    assert len(discovered_calendar) > 0
    assert 'Kim Passa' in discovered_calendar
    assert len(discovered_calendar['Kim Passa']) > 0


def test_calendar_make(assets_path):
    log_path = assets_path / 'non_processing_intervals.csv'
    event_log = core.read_csv(log_path)
    mined_calendar = calendar.make(event_log, granularity=60)
    assert mined_calendar is not None
    assert 'R1' in mined_calendar


def test_calendar_make_undifferentiated(assets_path):
    log_path = assets_path / 'undifferentiated_pool.csv'
    event_log = core.read_csv(log_path)
    mined_calendar = calendar.make(event_log, granularity=60, differentiated=False)
    assert mined_calendar is not None
    assert len(mined_calendar) == 1
    assert UNDIFFERENTIATED_RESOURCE_POOL_KEY in mined_calendar


def test_resource_work_time(log_calendar):
    resource = 'Kim Passa'
    time_intervals = calendar.resource_working_hours_as_intervals(resource, log_calendar)
    assert time_intervals is not None
    assert len(time_intervals) > 0


def test_overall_duration(log_calendar):
    resource = 'Kim Passa'
    work_time = calendar.resource_working_hours_as_intervals(resource, log_calendar)
    assert process_waste.calendar.intervals.overall_duration(work_time) > pd.Timedelta(0)


def test_intervals_overlap_a():
    working_hours_intervals = [
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='09:00:00', right_time='12:00:00'),
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='13:00:00', right_time='19:00:00'),
    ]

    wt_intervals = [
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='11:00:00', right_time='14:00:00'),
    ]

    intersecting_intervals = [
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='11:00:00', right_time='12:00:00'),
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='13:00:00', right_time='14:00:00'),
    ]

    result = intersect_intervals(working_hours_intervals, wt_intervals)
    assert result == intersecting_intervals


def test_intervals_overlap_b():
    working_hours_intervals = [
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='09:00:00', right_time='12:00:00'),
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='13:00:00', right_time='19:00:00'),
    ]

    wt_intervals = [
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='12:00:00', right_time='13:00:00'),
    ]

    intersecting_intervals = []

    result = intersect_intervals(working_hours_intervals, wt_intervals)
    assert result == intersecting_intervals


def test_intervals_overlap_c():
    working_hours_intervals = [
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='09:00:00', right_time='12:00:00'),
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='13:00:00', right_time='19:00:00'),
    ]

    wt_intervals = [
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='10:00:00', right_time='11:00:00'),
    ]

    intersecting_intervals = [
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='10:00:00', right_time='11:00:00'),
    ]

    result = intersect_intervals(working_hours_intervals, wt_intervals)
    assert result == intersecting_intervals


def test_intervals_overlap_d():
    # different days

    working_hours_intervals = [
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='09:00:00', right_time='12:00:00'),
        Interval(left_day=WeekDay.TUESDAY, right_day=WeekDay.TUESDAY, left_time='13:00:00', right_time='19:00:00'),
    ]

    wt_intervals = [
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='10:00:00', right_time='11:00:00'),
    ]

    intersecting_intervals = [
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='10:00:00', right_time='11:00:00'),
    ]

    result = intersect_intervals(working_hours_intervals, wt_intervals)
    assert result == intersecting_intervals


def test_subtract():
    interval1 = Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='09:00:00', right_time='12:00:00')
    interval2 = Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='11:00:00', right_time='13:00:00')
    expected_result = [
        Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='09:00:00', right_time='11:00:00'),
    ]
    result = interval1.subtract(interval2)
    assert result == expected_result

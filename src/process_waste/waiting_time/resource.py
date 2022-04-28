from typing import List

import pandas as pd
from tqdm import tqdm

import process_waste.calendar.intervals
from process_waste import START_TIMESTAMP_KEY, ENABLED_TIMESTAMP_KEY, RESOURCE_KEY, END_TIMESTAMP_KEY, \
    WAITING_TIME_UNAVAILABILITY_KEY
from process_waste.calendar import calendar
from process_waste.calendar.calendar import UNDIFFERENTIATED_RESOURCE_POOL_KEY
from process_waste.calendar.intervals import pd_interval_to_interval, Interval, subtract_intervals


def run_analysis(log: pd.DataFrame) -> pd.DataFrame:
    log[WAITING_TIME_UNAVAILABILITY_KEY] = pd.Timedelta(0)
    log_calendar = calendar.make(log, granularity=30, differentiated=False)
    for i in tqdm(log.index, desc='Resource unavailability analysis'):
        index = pd.Index([i])
        detect_waiting_time_due_to_unavailability(index, log, log_calendar)
    return log


def other_processing_events_during_waiting_time_of_event(event_index: pd.Index, log: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a dataframe with all other processing events that are in the waiting time of the given event, i.e.,
    activities that have been started before event_start_time but after event_enabled_time.

    :param event_index: Index of the event for which the waiting time is taken into account.
    :param log: Log dataframe.
    """
    event = log.loc[event_index]
    if isinstance(event, pd.Series):
        event = event.to_frame().T

    # current event variables
    event_start_time = event[START_TIMESTAMP_KEY].values[0]
    event_start_time = pd.to_datetime(event_start_time, utc=True)
    event_enabled_time = event[ENABLED_TIMESTAMP_KEY].values[0]
    event_enabled_time = pd.to_datetime(event_enabled_time, utc=True)
    resource = event[RESOURCE_KEY].values[0]

    # resource events throughout the event log except the current event
    resource_events = log[log[RESOURCE_KEY] == resource]
    resource_events = resource_events.loc[resource_events.index.difference(event_index)]

    # taking activities that resource started before event_start_time but after event_enabled_time
    other_processing_events = resource_events[
        (resource_events[START_TIMESTAMP_KEY] < event_start_time) &
        (resource_events[END_TIMESTAMP_KEY] >= event_enabled_time)]

    return other_processing_events


def non_processing_intervals(event_index: pd.Index, log: pd.DataFrame) -> List[Interval]:
    """
    Returns a list of intervals during which no processing has taken place.

    :param event_index: Index of the event for which the waiting time is taken into account.
    :param log: Log dataframe.
    """
    event = log.loc[event_index]
    if isinstance(event, pd.Series):
        event = event.to_frame().T

    # current event variables
    event_start_time = event[START_TIMESTAMP_KEY].values[0]
    event_start_time = pd.to_datetime(event_start_time, utc=True)
    event_enabled_time = event[ENABLED_TIMESTAMP_KEY].values[0]
    event_enabled_time = pd.to_datetime(event_enabled_time, utc=True)
    wt_interval = pd.Interval(event_enabled_time, event_start_time)
    wt_interval = pd_interval_to_interval(wt_interval)

    other_processing_events = other_processing_events_during_waiting_time_of_event(event_index, log)
    if len(other_processing_events) == 0:
        return wt_interval

    other_processing_events_intervals = []
    for (_, event) in other_processing_events.iterrows():
        pd_interval = pd.Interval(event[START_TIMESTAMP_KEY], event[END_TIMESTAMP_KEY])
        interval = pd_interval_to_interval(pd_interval)
        other_processing_events_intervals.extend(interval)

    result = subtract_intervals(wt_interval, other_processing_events_intervals)

    return result


def detect_waiting_time_due_to_unavailability(
        event_index: pd.Index,
        log: pd.DataFrame,
        log_calendar: dict,
        differentiated=True):
    event = log.loc[event_index]
    if isinstance(event, pd.Series):
        event = event.to_frame().T

    idle_intervals = non_processing_intervals(event_index, log)

    if differentiated:
        resource = event[RESOURCE_KEY].values[0]
    else:
        resource = UNDIFFERENTIATED_RESOURCE_POOL_KEY
    # TODO: this are working hours for all the time, we need only for this particular case
    overall_work_intervals = calendar.resource_working_hours_as_intervals(resource, log_calendar)

    # NOTE: we assume that case events happen during the same day
    start_time = pd.Timestamp(event[START_TIMESTAMP_KEY].values[0])
    enabled_time = pd.Timestamp(event[ENABLED_TIMESTAMP_KEY].values[0])
    if not start_time.tz:  # TODO: is there a better place to do tz localization?
        tz = enabled_time.tz if enabled_time.tz else 'UTC'
        start_time = start_time.tz_localize(tz)
    if not enabled_time.tz:
        tz = start_time.tz if start_time.tz else 'UTC'
        enabled_time = enabled_time.tz_localize(tz)

    wt_intervals = process_waste.calendar.intervals.pd_interval_to_interval(pd.Interval(enabled_time, start_time))
    working_hours_during_wt = process_waste.calendar.intervals.intersect_intervals(wt_intervals, overall_work_intervals)
    unavailability_intervals = process_waste.calendar.intervals.subtract_intervals(idle_intervals, working_hours_during_wt)
    wt_due_to_resource_unavailability = process_waste.calendar.intervals.overall_duration(unavailability_intervals)
    log.loc[event_index, WAITING_TIME_UNAVAILABILITY_KEY] = wt_due_to_resource_unavailability


# def _split_intervals_which_are_across_several_days(intervals: List[pd.Interval]) -> List[pd.Interval]:
#     """
#     Splits intervals which are across several days.
#
#     :param intervals: List of intervals.
#     :return: List of intervals.
#     """
#     intervals_splits = []
#     for interval in intervals:
#         if interval.left.day != interval.right.day:
#             new_left = interval.left
#             new_left = new_left.replace(hour=23, minute=59, second=59, microsecond=999999)
#             new_right = interval.right
#             new_right = new_right.replace(hour=0, minute=0, second=0, microsecond=0)
#             intervals_splits.extend([
#                 pd.Interval(interval.left, new_left),
#                 pd.Interval(new_right, interval.right)
#             ])
#         else:
#             intervals_splits.append(interval)
#     return intervals_splits


def _subtract_intervals_from_interval(interval: pd.Interval, intervals: [pd.Interval]) -> List[pd.Interval]:
    """
    Subtracts all intervals from the given interval.

    :param interval: Interval from which the intervals are subtracted.
    :param intervals: Intervals that are subtracted from the given interval.
    """
    interval_splits = [interval]
    for item in intervals:
        last_split = interval_splits.pop()
        interval_splits.extend(_subtract_interval(last_split, item))
    interval_splits = list(set(interval_splits))
    return interval_splits


def _subtract_interval(interval: pd.Interval, split_interval: pd.Interval) -> List[pd.Interval]:
    """
    Splits an interval into two intervals.

    :param interval: Interval to be split.
    :param split_interval: Time interval that should be excluded from the interval.
    :return: List of two intervals.
    """
    if (interval.left <= split_interval.left <= interval.right) and \
            (interval.left <= split_interval.right <= interval.right):
        result = []
        left_interval = pd.Interval(interval.left, split_interval.left)
        if left_interval.right != left_interval.left:
            result.append(left_interval)
        right_interval = pd.Interval(split_interval.right, interval.right)
        if right_interval.right != right_interval.left:
            result.append(right_interval)
        return result
    else:
        return [interval]
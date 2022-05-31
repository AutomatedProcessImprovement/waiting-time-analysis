from typing import List

import pandas as pd
from tqdm import tqdm

from process_waste import START_TIMESTAMP_KEY, ENABLED_TIMESTAMP_KEY, RESOURCE_KEY, END_TIMESTAMP_KEY, \
    WAITING_TIME_UNAVAILABILITY_KEY
from process_waste.calendar import calendar
from process_waste.calendar.calendar import UNDIFFERENTIATED_RESOURCE_POOL_KEY
from process_waste.calendar.intervals import pd_interval_to_interval, Interval, subtract_intervals, \
    intersect_intervals, overall_duration


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

    wt_intervals = pd_interval_to_interval(pd.Interval(enabled_time, start_time))
    working_hours_during_wt = intersect_intervals(wt_intervals, overall_work_intervals)
    unavailability_intervals = subtract_intervals(idle_intervals, working_hours_during_wt)
    wt_due_to_resource_unavailability = overall_duration(unavailability_intervals)
    log.loc[event_index, WAITING_TIME_UNAVAILABILITY_KEY] = wt_due_to_resource_unavailability


def detect_waiting_times_due_to_unavailability(log: pd.DataFrame, log_calendar: dict):
    for i in log.index:
        event_index = pd.Index([i])
        detect_waiting_time_due_to_unavailability(event_index, log, log_calendar)

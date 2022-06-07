from typing import List, Optional

import pandas as pd

from batch_processing_analysis.config import EventLogIDs
from process_waste import log_ids_non_nil
from process_waste.calendar import calendar
from process_waste.calendar.calendar import UNDIFFERENTIATED_RESOURCE_POOL_KEY
from process_waste.calendar.intervals import pd_interval_to_interval, Interval, subtract_intervals, \
    intersect_intervals


def other_processing_events_during_waiting_time_of_event(
        event_index: pd.Index,
        log: pd.DataFrame,
        log_ids: Optional[EventLogIDs] = None) -> pd.DataFrame:
    """
    Returns a dataframe with all other processing events that are in the waiting time of the given event, i.e.,
    activities that have been started before event_start_time but after event_enabled_time.

    :param event_index: Index of the event for which the waiting time is taken into account.
    :param log: Log dataframe.
    :param log_ids: Event log IDs.
    """
    log_ids = log_ids_non_nil(log_ids)

    event = log.loc[event_index]
    if isinstance(event, pd.Series):
        event = event.to_frame().T

    # current event variables
    event_start_time = event[log_ids.start_time].values[0]
    event_start_time = pd.to_datetime(event_start_time, utc=True)
    event_enabled_time = event[log_ids.enabled_time].values[0]
    event_enabled_time = pd.to_datetime(event_enabled_time, utc=True)
    resource = event[log_ids.resource].values[0]

    # resource events throughout the event log except the current event
    resource_events = log[log[log_ids.resource] == resource]
    resource_events = resource_events.loc[resource_events.index.difference(event_index)]

    # taking activities that resource started before event_start_time but after event_enabled_time
    other_processing_events = resource_events[
        (resource_events[log_ids.start_time] < event_start_time) &
        (resource_events[log_ids.end_time] > event_enabled_time)]

    return other_processing_events


def non_processing_intervals(
        event_index: pd.Index,
        log: pd.DataFrame,
        log_ids: Optional[EventLogIDs] = None) -> List[Interval]:
    """
    Returns a list of intervals during which no processing has taken place.

    :param event_index: Index of the event for which the waiting time is taken into account.
    :param log: Log dataframe.
    :param log_ids: Event log IDs.
    """
    log_ids = log_ids_non_nil(log_ids)

    event = log.loc[event_index]
    if isinstance(event, pd.Series):
        event = event.to_frame().T

    # current event variables
    event_start_time = event[log_ids.start_time].values[0]
    event_start_time = pd.to_datetime(event_start_time, utc=True)
    event_enabled_time = event[log_ids.enabled_time].values[0]
    event_enabled_time = pd.to_datetime(event_enabled_time, utc=True)
    wt_interval = pd.Interval(event_enabled_time, event_start_time)
    wt_interval = pd_interval_to_interval(wt_interval)

    other_processing_events = other_processing_events_during_waiting_time_of_event(event_index, log, log_ids=log_ids)
    if len(other_processing_events) == 0:
        return wt_interval

    other_processing_events_intervals = []
    for (_, event) in other_processing_events.iterrows():
        pd_interval = pd.Interval(event[log_ids.start_time], event[log_ids.end_time])
        interval = pd_interval_to_interval(pd_interval)
        other_processing_events_intervals.extend(interval)

    result = subtract_intervals(wt_interval, other_processing_events_intervals)

    return result


def detect_unavailability_intervals(
        event_index: pd.Index,
        log: pd.DataFrame,
        log_calendar: dict,
        differentiated=True,
        log_ids: Optional[EventLogIDs] = None) -> List[Interval]:
    log_ids = log_ids_non_nil(log_ids)

    event = log.loc[event_index]
    if isinstance(event, pd.Series):
        event = event.to_frame().T

    if differentiated:
        resource = event[log_ids.resource].values[0]
    else:
        resource = UNDIFFERENTIATED_RESOURCE_POOL_KEY

    start_time = pd.Timestamp(event[log_ids.start_time].values[0])
    enabled_time = pd.Timestamp(event[log_ids.enabled_time].values[0])
    # TODO: do tz localization somewhere earlier (preprocessing module) on the whole log
    start_time = __ensure_timestamp_tz(start_time, enabled_time.tz)
    enabled_time = __ensure_timestamp_tz(enabled_time, start_time.tz)

    overall_work_intervals = calendar.resource_working_hours_as_intervals(resource, log_calendar)

    wt_intervals = pd_interval_to_interval(pd.Interval(enabled_time, start_time))

    working_hours_during_wt = intersect_intervals(wt_intervals, overall_work_intervals)

    idle_intervals = non_processing_intervals(event_index, log, log_ids=log_ids)

    unavailability_intervals = subtract_intervals(idle_intervals, working_hours_during_wt)

    return unavailability_intervals


def __ensure_timestamp_tz(timestamp: pd.Timestamp, tz: Optional[str] = None):
    if not timestamp.tz:
        tz = tz if tz else 'UTC'
        timestamp = timestamp.tz_localize(tz)

    return timestamp

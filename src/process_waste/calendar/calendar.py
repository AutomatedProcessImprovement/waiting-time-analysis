import pandas as pd

from bpdfr_simulation_engine.resource_calendar import CalendarFactory

from process_waste import RESOURCE_KEY, ACTIVITY_KEY, START_TIMESTAMP_KEY, END_TIMESTAMP_KEY
from process_waste.calendar.intervals import Interval, prosimos_interval_to_interval

UNDIFFERENTIATED_RESOURCE_POOL_KEY = "undifferentiated_resource_pool"


def make(event_log: pd.DataFrame,
         granularity=60,
         min_confidence=0.1,
         desired_support=0.7,
         min_participation=0.4,
         differentiated=True) -> dict:
    """
    Creates a calendar for the given event log using Prosimos. If the amount of event is too low, the results are not
    trustworthy. It's recommended to build a resource calendar for the whole resource pool instead of a single resource.
    The more the events there is in the log, the smaller the granularity should be.

    :param event_log: The event log to use.
    :param granularity: The number of minutes that is added to the start timestamp.
    :param min_confidence: The minimum confidence.
    :param desired_support: The desired support.
    :param min_participation: The minimum participation.
    :param differentiated: Whether to mine differentiated calendars for each resource or to use a single resource pool for all resources.
    :return: the calendar dictionary with the resource names as keys and the working time intervals as values.
    """
    calendar_factory = CalendarFactory(granularity)
    for (index, event) in event_log.iterrows():
        if differentiated:
            resource = event[RESOURCE_KEY]
        else:
            resource = UNDIFFERENTIATED_RESOURCE_POOL_KEY
        activity = event[ACTIVITY_KEY]
        start_time = event[START_TIMESTAMP_KEY]
        end_time = event[END_TIMESTAMP_KEY]
        calendar_factory.check_date_time(resource, activity, start_time)
        calendar_factory.check_date_time(resource, activity, end_time)
    calendar_candidates = calendar_factory.build_weekly_calendars(min_confidence, desired_support, min_participation)
    calendar = {}
    for resource_id in calendar_candidates:
        if calendar_candidates[resource_id] is not None:
            calendar[resource_id] = calendar_candidates[resource_id].to_json()
    return calendar


def resource_working_hours_as_intervals(resource: str, calendar: dict) -> [Interval]:
    """
    Computes the working time intervals of a resource.

    :param resource: the resource name to calculate the working hours for.
    :param calendar: the calendar to use generated by Prosimos.
    :return: working time intervals of the resource.
    """
    intervals = calendar.get(resource, [])
    if len(intervals) == 0:
        return []

    new_intervals = [prosimos_interval_to_interval(interval) for interval in intervals]
    return new_intervals


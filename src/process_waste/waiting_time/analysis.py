from collections import namedtuple
from typing import Optional, List, Tuple

import pandas as pd

from batch_processing_analysis.config import EventLogIDs
from process_waste import log_ids_non_nil, WAITING_TIME_TOTAL_KEY, WAITING_TIME_CONTENTION_KEY, \
    WAITING_TIME_BATCHING_KEY, WAITING_TIME_PRIORITIZATION_KEY, WAITING_TIME_UNAVAILABILITY_KEY, \
    WAITING_TIME_EXTRANEOUS_KEY, BATCH_INSTANCE_ENABLED_KEY
from process_waste.calendar.intervals import Interval
from process_waste.waiting_time.prioritization_and_contention import detect_contention_and_prioritization_intervals
from process_waste.waiting_time.resource_unavailability import detect_unavailability_intervals

TRANSITION_COLUMN_KEY = 'transition_source_index'


def run(case: pd.DataFrame,
        log_calendar: dict,
        log: Optional[pd.DataFrame] = None,
        log_ids: Optional[EventLogIDs] = None) -> pd.DataFrame:
    """Runs the waiting time analysis on transitions of the given case."""

    log_ids = log_ids_non_nil(log_ids)

    # preparing a different dataframe for handoff reporting
    columns = ['source_activity', 'source_resource', 'destination_activity', 'destination_resource',
               WAITING_TIME_TOTAL_KEY, 'handoff_type', WAITING_TIME_CONTENTION_KEY]
    transitions = pd.DataFrame(columns=columns)

    transitions_index = case[~case[TRANSITION_COLUMN_KEY].isna()].index

    # setting NaT to zero duration
    if WAITING_TIME_CONTENTION_KEY in case.columns:
        case.loc[case[case[WAITING_TIME_CONTENTION_KEY].isna()].index, WAITING_TIME_CONTENTION_KEY] = pd.Timedelta(0)

    for loc in transitions_index:
        destination = case.loc[loc]
        destination_index = pd.Index([loc])
        source_index = int(destination[TRANSITION_COLUMN_KEY])
        source = case.loc[source_index]

        # NOTE: for WT analysis we take only the destination activity, the source activity is not relevant

        wt_total = destination[WAITING_TIME_TOTAL_KEY]
        if wt_total > pd.Timedelta(0):
            # Perform analysis
            wt_batching_interval = __wt_batching_interval(destination, log_ids)
            wt_contention_intervals, wt_prioritization_intervals = \
                __wt_contention_and_prioritization_intervals(destination_index, log, log_ids)
            wt_unavailability_intervals = __wt_unavailability_intervals(destination_index, log, log_calendar, log_ids)

            wt_analysis = __wt_durations_from_wt_intervals(
                wt_batching_interval,
                wt_contention_intervals,
                wt_prioritization_intervals,
                wt_unavailability_intervals, wt_total)

            wt_batching = wt_analysis.batching
            wt_contention = wt_analysis.contention
            wt_prioritization = wt_analysis.prioritization
            wt_unavailability = wt_analysis.unavailability
            wt_extraneous = wt_analysis.extraneous
        else:
            # There is no WT, so don't run analysis
            wt_batching = pd.Timedelta(0)
            wt_contention = pd.Timedelta(0)
            wt_prioritization = pd.Timedelta(0)
            wt_unavailability = pd.Timedelta(0)
            wt_extraneous = pd.Timedelta(0)

        transition_type = 'transition'

        # appending the handoff data
        handoff = pd.DataFrame({
            'source_activity': [source[log_ids.activity]],
            'source_resource': [source[log_ids.resource]],
            'destination_activity': [destination[log_ids.activity]],
            'destination_resource': [destination[log_ids.resource]],
            'handoff_type': [transition_type],
            WAITING_TIME_TOTAL_KEY: [wt_total],
            WAITING_TIME_BATCHING_KEY: [wt_batching],
            WAITING_TIME_PRIORITIZATION_KEY: [wt_prioritization],
            WAITING_TIME_CONTENTION_KEY: [wt_contention],
            WAITING_TIME_UNAVAILABILITY_KEY: [wt_unavailability],
            WAITING_TIME_EXTRANEOUS_KEY: [wt_extraneous]
        })
        transitions = pd.concat([transitions, handoff], ignore_index=True)

    # filling in N/A with some values
    transitions['source_resource'] = transitions['source_resource'].fillna('NA')
    transitions['destination_resource'] = transitions['destination_resource'].fillna('NA')
    return transitions


def __wt_unavailability_intervals(destination_index, log, log_calendar, log_ids) -> List[Interval]:
    """Discovers waiting time due to unavailability of resources."""

    wt_unavailability_intervals = detect_unavailability_intervals(destination_index, log, log_calendar, log_ids=log_ids)

    intervals = list(filter(lambda interval: interval.length > pd.Timedelta(0), wt_unavailability_intervals))
    return None if len(intervals) == 0 else intervals


def __wt_contention_and_prioritization_intervals(
        destination_index: pd.Index,  # handoff pair has source and destination activities
        log: pd.DataFrame,
        log_ids: EventLogIDs) -> Tuple[Tuple[List, List], Tuple[List, List]]:
    """Discovers waiting time due to resource contention and prioritization waiting times."""

    # NOTE: WT of the destination activity is relevant only
    wt_contention_intervals, wt_prioritization_intervals = \
        detect_contention_and_prioritization_intervals(destination_index, log, log_ids=log_ids)

    return wt_contention_intervals, wt_prioritization_intervals


def __wt_batching_interval(
        destination: pd.DataFrame,
        log_ids: EventLogIDs) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Discovers waiting time due to batching."""

    if BATCH_INSTANCE_ENABLED_KEY not in destination.index:
        return None

    batch_enabled_timestamp = destination[BATCH_INSTANCE_ENABLED_KEY]
    event_enabled_timestamp = destination[log_ids.enabled_time]
    if pd.isna(event_enabled_timestamp) or pd.isna(batch_enabled_timestamp):
        return None

    if destination[log_ids.enabled_time] > destination[BATCH_INSTANCE_ENABLED_KEY]:
        return None

    return destination[log_ids.enabled_time], destination[BATCH_INSTANCE_ENABLED_KEY]


WaitingTimeDurations = namedtuple(
    'ComputedWaitingTimes',
    ['batching', 'contention', 'prioritization', 'unavailability', 'extraneous']
)


def __wt_durations_from_wt_intervals(
        wt_batching_interval: Optional[Tuple[pd.Timestamp, pd.Timestamp]],
        wt_contention_intervals: Optional[Tuple[List, List]],
        wt_prioritization_intervals: Optional[Tuple[List, List]],
        wt_unavailability_intervals: Optional[List[Interval]],
        wt_total: pd.Timedelta) -> WaitingTimeDurations:
    """Computes waiting time while taking into account overlapping intervals."""

    wt_batching = pd.Timedelta(0)
    wt_contention = pd.Timedelta(0)
    wt_prioritization = pd.Timedelta(0)
    wt_unavailability = pd.Timedelta(0)
    wt_extraneous = wt_total

    def make_intervals(_intervals: Tuple[List, list]) -> List[pd.Interval]:
        _intervals = list(zip(_intervals[0], _intervals[1]))

        _intervals = [
            pd.Interval(pd.to_datetime(start, utc=True), pd.to_datetime(end, utc=True))
            for start, end in _intervals
        ]

        _intervals = __remove_overlapping_time_from_intervals(_intervals)

        return _intervals

    if not wt_batching_interval and not wt_contention_intervals and not wt_prioritization_intervals and not wt_unavailability_intervals:
        return WaitingTimeDurations(wt_batching, wt_contention, wt_prioritization, wt_unavailability, wt_extraneous)

    # waiting time analysis

    wt_contention_pd_intervals = None
    wt_prioritization_pd_intervals = None

    # batching calculation

    if wt_batching_interval:
        wt_batching = wt_batching_interval[1] - wt_batching_interval[0]

    # contention calculation

    if wt_contention_intervals:
        wt_contention_pd_intervals = make_intervals(wt_contention_intervals)

        if wt_batching == pd.Timedelta(0):
            wt_contention = __duration_of_pd_intervals(wt_contention_pd_intervals)
        else:
            wt_batching_pd_interval = pd.Interval(wt_batching_interval[0], wt_batching_interval[1])
            wt_contention_pd_intervals = \
                __subtract_a_from_intervals_b(wt_batching_pd_interval, wt_contention_pd_intervals)

            wt_contention = __duration_of_pd_intervals(wt_contention_pd_intervals)

    # prioritization calculation

    if wt_prioritization_intervals:
        wt_prioritization_pd_intervals = make_intervals(wt_prioritization_intervals)

        if wt_batching > pd.Timedelta(0):
            wt_batching_pd_interval = pd.Interval(wt_batching_interval[0], wt_batching_interval[1])
            wt_prioritization_pd_intervals = \
                __subtract_a_from_intervals_b(wt_batching_pd_interval, wt_prioritization_pd_intervals)

        if wt_contention_pd_intervals:
            wt_prioritization_pd_intervals = \
                __subtract_intervals_a_from_intervals_b(wt_contention_pd_intervals,
                                                        wt_prioritization_pd_intervals)

        wt_prioritization = __duration_of_pd_intervals(wt_prioritization_pd_intervals)

    # unavailability calculation

    if wt_unavailability_intervals:
        if wt_batching > pd.Timedelta(0):
            wt_batching_pd_interval = pd.Interval(wt_batching_interval[0], wt_batching_interval[1])
            wt_unavailability_intervals = \
                __subtract_a_from_intervals_b(wt_batching_pd_interval, wt_unavailability_intervals)

        if wt_contention_pd_intervals:
            wt_unavailability_intervals = __subtract_intervals_a_from_intervals_b(
                wt_contention_pd_intervals, wt_unavailability_intervals
            )

        if wt_prioritization_pd_intervals:
            wt_unavailability_intervals = __subtract_intervals_a_from_intervals_b(
                wt_prioritization_pd_intervals, wt_unavailability_intervals
            )

        wt_unavailability = __duration_of_pd_intervals(wt_unavailability_intervals)

    # extraneous calculation

    wt_extraneous = wt_total - wt_batching - wt_contention - wt_prioritization - wt_unavailability

    return WaitingTimeDurations(wt_batching, wt_contention, wt_prioritization, wt_unavailability, wt_extraneous)


# Pandas Intervals

def __subtract_a_from_b(a: pd.Interval, b: pd.Interval) -> [pd.Interval]:
    """Subtracts the interval a from b."""

    if not a.overlaps(b):
        return [b]

    if a.left == b.left:
        if a.right == b.right:
            return []

        if a.right < b.right:
            return [pd.Interval(a.right, b.right)]

        if a.right > b.right:
            return []

    if a.right == b.right:
        if a.left > b.left:
            return [pd.Interval(b.left, a.left)]

        if a.left < b.left:
            return []

    if a.left < b.left:
        if a.right < b.right:
            return [pd.Interval(a.right, b.right)]
        if a.right > b.right:
            return []

    if a.left > b.left:
        if a.right > b.right:
            return [pd.Interval(b.left, a.left)]

        if a.right < b.right:
            return [pd.Interval(b.left, a.left), pd.Interval(a.right, b.right)]


def __subtract_a_from_intervals_b(a: pd.Interval, b: List[pd.Interval]) -> List[pd.Interval]:
    """Subtracts the interval a from intervals b."""

    if len(b) == 0:
        return []

    result = []
    for interval_b in b:
        result.extend(__subtract_a_from_b(a, interval_b))

    return result


def __subtract_intervals_a_from_intervals_b(a: List[pd.Interval], b: List[pd.Interval]) -> List[pd.Interval]:
    """Subtracts the intervals a from b."""

    if len(a) == 0:
        return b

    if len(b) == 0:
        return []

    result = __subtract_a_from_intervals_b(a[0], b)

    if len(a) == 1:
        return result

    return __subtract_intervals_a_from_intervals_b(a[1:], result)


def __remove_overlapping_time_from_intervals(a: List[pd.Interval]) -> List[pd.Interval]:
    """Removes overlapping time from intervals."""

    if len(a) == 0:
        return []

    if len(a) == 1:
        return a

    a = sorted(a, key=lambda x: x.left)

    result = __subtract_a_from_intervals_b(a[0], a[1:])

    return [a[0]] + __remove_overlapping_time_from_intervals(result)


def __duration_of_pd_intervals(intervals: List[pd.Interval]) -> pd.Timedelta:
    """Returns the duration of the intervals."""

    if len(intervals) == 0:
        return pd.Timedelta(0)

    result = pd.Timedelta(0)
    for interval in intervals:
        result += interval.right - interval.left

    return result

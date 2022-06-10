from collections import namedtuple
from typing import Dict, Optional, Tuple, List

import click
import pandas as pd

from batch_processing_analysis.config import EventLogIDs
from process_waste import core, WAITING_TIME_TOTAL_KEY, WAITING_TIME_BATCHING_KEY, WAITING_TIME_CONTENTION_KEY, \
    WAITING_TIME_PRIORITIZATION_KEY, WAITING_TIME_UNAVAILABILITY_KEY, WAITING_TIME_EXTRANEOUS_KEY, \
    convert_timestamp_columns_to_datetime, log_ids_non_nil, BATCH_INSTANCE_ENABLED_KEY, print_section_boundaries
from process_waste.calendar.intervals import Interval, pd_interval_to_interval, subtract_intervals, \
    pd_intervals_to_intervals, overall_duration
from process_waste.waiting_time.prioritization_and_contention import detect_contention_and_prioritization_intervals
from process_waste.waiting_time.resource_unavailability import detect_unavailability_intervals


@print_section_boundaries('Hand-off analysis')
def identify(
        log: pd.DataFrame,
        parallel_activities: Dict[str, set],
        parallel_run=True,
        log_ids: Optional[EventLogIDs] = None,
        calendar: Optional[Dict] = None) -> pd.DataFrame:
    click.echo(f'Handoff identification. Parallel run: {parallel_run}')
    result = core.identify_main(
        log=log,
        parallel_activities=parallel_activities,
        identify_fn_per_case=__identify_handoffs_per_case_and_make_report,
        join_fn=core.join_per_case_items,
        parallel_run=parallel_run,
        log_ids=log_ids,
        calendar=calendar)
    return result


def __identify_handoffs_per_case_and_make_report(case: pd.DataFrame, **kwargs) -> pd.DataFrame:
    parallel_activities = kwargs['parallel_activities']
    case_id = kwargs['case_id']
    log_calendar = kwargs['log_calendar']
    log = kwargs['log']
    log_ids = log_ids_non_nil(kwargs.get('log_ids'))

    case = case.sort_values(by=[log_ids.end_time, log_ids.start_time]).copy()
    case.reset_index()

    # converting timestamps to datetime
    log = convert_timestamp_columns_to_datetime(log, log_ids)
    case = convert_timestamp_columns_to_datetime(case, log_ids)

    __mark_strict_handoffs(case, parallel_activities, log_ids=log_ids)
    __mark_self_handoffs(case, parallel_activities, log_ids=log_ids)
    potential_handoffs = case[~case['handoff_type'].isna()]

    handoffs_index = potential_handoffs.index
    handoffs = __make_report(case, handoffs_index, log_calendar, log, log_ids=log_ids)

    handoffs_with_frequency = __calculate_frequency_and_duration(handoffs)

    # dropping edge cases with Start and End as an activity
    starts_ends_values = ['Start', 'End']
    starts_and_ends = (handoffs_with_frequency['source_activity'].isin(starts_ends_values)
                       & handoffs_with_frequency['source_resource'].isin(starts_ends_values)) \
                      | (handoffs_with_frequency['destination_activity'].isin(starts_ends_values)
                         & handoffs_with_frequency['destination_resource'].isin(starts_ends_values))
    handoffs_with_frequency = handoffs_with_frequency[starts_and_ends == False]

    # attaching case ID as additional information
    handoffs_with_frequency['case_id'] = case_id

    return handoffs_with_frequency


def __mark_strict_handoffs(
        case: pd.DataFrame,
        parallel_activities: Optional[Dict[str, set]] = None,
        log_ids: Optional[EventLogIDs] = None) -> pd.DataFrame:
    log_ids = log_ids_non_nil(log_ids)

    # TODO: should ENABLED_TIMESTAMP_KEY be used?

    # checking the main conditions for handoff to occur
    next_events = case.shift(-1)
    resource_changed = case[log_ids.resource] != next_events[log_ids.resource]
    activity_changed = case[log_ids.activity] != next_events[log_ids.activity]
    consecutive_timestamps = case[log_ids.end_time] <= next_events[log_ids.start_time]
    not_parallel = pd.Series(index=case.index, dtype=bool)
    prev_activities = case[log_ids.activity]
    next_activities = next_events[log_ids.activity]
    for (i, pair) in enumerate(zip(prev_activities, next_activities)):
        if pair[0] == pair[1]:
            not_parallel.iat[i] = False
            continue
        parallel_set = parallel_activities.get(pair[1], None) if parallel_activities else None
        if parallel_set and pair[0] in parallel_set:
            not_parallel.iat[i] = False
        else:
            not_parallel.iat[i] = True
    not_parallel = pd.Series(not_parallel)
    handoff_occurred = resource_changed & activity_changed & consecutive_timestamps & not_parallel
    case.at[handoff_occurred, 'handoff_type'] = 'strict'
    return case


def __mark_self_handoffs(
        case: pd.DataFrame,
        parallel_activities: Optional[Dict[str, set]] = None,
        log_ids: Optional[EventLogIDs] = None) -> pd.DataFrame:
    log_ids = log_ids_non_nil(log_ids)

    # TODO: should ENABLED_TIMESTAMP_KEY be used?

    # checking the main conditions for handoff to occur
    next_events = case.shift(-1)
    same_resource = case[log_ids.resource] == next_events[log_ids.resource]
    activity_changed = case[log_ids.activity] != next_events[log_ids.activity]
    consecutive_timestamps = case[log_ids.end_time] <= next_events[log_ids.start_time]
    not_parallel = pd.Series(index=case.index, dtype=bool)
    prev_activities = case[log_ids.activity]
    next_activities = next_events[log_ids.activity]
    for (i, pair) in enumerate(zip(prev_activities, next_activities)):
        if pair[0] == pair[1]:
            not_parallel.iat[i] = False
            continue
        parallel_set = parallel_activities.get(pair[1], None) if parallel_activities else None
        if parallel_set and pair[0] in parallel_set:
            not_parallel.iat[i] = False
        else:
            not_parallel.iat[i] = True
    not_parallel = pd.Series(not_parallel)
    handoff_occurred = same_resource & activity_changed & consecutive_timestamps & not_parallel
    case.loc[case[handoff_occurred].index, 'handoff_type'] = 'self'
    return case


def __calculate_frequency_and_duration(handoffs: pd.DataFrame) -> pd.DataFrame:
    # calculating frequency per case of the handoffs with the same activities and resources
    columns = handoffs.columns.tolist()
    handoff_with_frequency = pd.DataFrame(columns=columns)
    handoff_grouped = handoffs.groupby(by=[
        'source_activity', 'source_resource', 'destination_activity', 'destination_resource'
    ])
    for group in handoff_grouped:
        pair, records = group
        handoff_with_frequency = pd.concat([handoff_with_frequency, pd.DataFrame({
            'source_activity': [pair[0]],
            'source_resource': [pair[1]],
            'destination_activity': [pair[2]],
            'destination_resource': [pair[3]],
            'frequency': [len(records)],
            'handoff_type': [records['handoff_type'].iloc[0]],
            WAITING_TIME_TOTAL_KEY: [records[WAITING_TIME_TOTAL_KEY].sum()],
            WAITING_TIME_BATCHING_KEY: [records[WAITING_TIME_BATCHING_KEY].sum()],
            WAITING_TIME_PRIORITIZATION_KEY: [records[WAITING_TIME_PRIORITIZATION_KEY].sum()],
            WAITING_TIME_CONTENTION_KEY: [records[WAITING_TIME_CONTENTION_KEY].sum()],
            WAITING_TIME_UNAVAILABILITY_KEY: [records[WAITING_TIME_UNAVAILABILITY_KEY].sum()],
            WAITING_TIME_EXTRANEOUS_KEY: [records[WAITING_TIME_EXTRANEOUS_KEY].sum()],
        })], ignore_index=True)
    return handoff_with_frequency


def __make_report(
        case: pd.DataFrame,
        handoffs_index: pd.Index,
        log_calendar: dict,
        log: Optional[pd.DataFrame] = None,
        log_ids: Optional[EventLogIDs] = None) -> pd.DataFrame:
    log_ids = log_ids_non_nil(log_ids)

    # preparing a different dataframe for handoff reporting
    columns = ['source_activity', 'source_resource', 'destination_activity', 'destination_resource',
               WAITING_TIME_TOTAL_KEY, 'handoff_type', WAITING_TIME_CONTENTION_KEY]
    handoffs = pd.DataFrame(columns=columns)

    # setting NaT to zero duration
    if WAITING_TIME_CONTENTION_KEY in case.columns:
        case.loc[case[case[WAITING_TIME_CONTENTION_KEY].isna()].index, WAITING_TIME_CONTENTION_KEY] = pd.Timedelta(0)

    for loc in handoffs_index:
        source = case.loc[loc]
        destination = case.loc[loc + 1]
        destination_index = pd.Index([loc + 1])

        # NOTE: for WT analysis we take only the destination activity, the source activity is not relevant

        wt_total = destination[WAITING_TIME_TOTAL_KEY]
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

        # handoff type identification
        if source[log_ids.resource] == destination[log_ids.resource]:
            handoff_type = 'self'
        else:
            handoff_type = 'strict'

        # appending the handoff data
        handoff = pd.DataFrame({
            'source_activity': [source[log_ids.activity]],
            'source_resource': [source[log_ids.resource]],
            'destination_activity': [destination[log_ids.activity]],
            'destination_resource': [destination[log_ids.resource]],
            'handoff_type': [handoff_type],
            WAITING_TIME_TOTAL_KEY: [wt_total],
            WAITING_TIME_BATCHING_KEY: [wt_batching],
            WAITING_TIME_PRIORITIZATION_KEY: [wt_prioritization],
            WAITING_TIME_CONTENTION_KEY: [wt_contention],
            WAITING_TIME_UNAVAILABILITY_KEY: [wt_unavailability],
            WAITING_TIME_EXTRANEOUS_KEY: [wt_extraneous]
        })
        handoffs = pd.concat([handoffs, handoff], ignore_index=True)

    # filling in N/A with some values
    handoffs['source_resource'] = handoffs['source_resource'].fillna('NA')
    handoffs['destination_resource'] = handoffs['destination_resource'].fillna('NA')
    return handoffs


# Waiting Time Analysis

def __wt_unavailability_intervals(destination_index, log, log_calendar, log_ids) -> List[Interval]:
    """Discovers waiting time due to unavailability of resources."""

    wt_unavailability_intervals = detect_unavailability_intervals(destination_index, log, log_calendar, log_ids=log_ids)

    def is_empty(interval: Interval) -> bool:
        if interval.left_time == interval.right_time:
            if interval.left_day == interval.right_day:
                return True
        return False

    intervals = list(filter(lambda interval: not is_empty(interval), wt_unavailability_intervals))
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
            for start, end in _intervals]
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
        wt_contention_start = pd.to_datetime(wt_contention_intervals[0], utc=True)
        wt_contention_end = pd.to_datetime(wt_contention_intervals[1], utc=True)
        if wt_batching == pd.Timedelta(0):
            wt_contention = (wt_contention_end - wt_contention_start).sum()
        else:
            wt_batching_pd_interval = pd.Interval(wt_batching_interval[0], wt_batching_interval[1])
            wt_contention_pd_intervals = make_intervals(wt_contention_intervals)
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
                __subtract_intervals_a_from_intervals_b(wt_contention_pd_intervals, wt_prioritization_pd_intervals)

        wt_prioritization = __duration_of_pd_intervals(wt_prioritization_pd_intervals)

    # unavailability calculation

    if wt_unavailability_intervals:
        if wt_batching > pd.Timedelta(0):
            wt_batching_custom_interval = pd_interval_to_interval(
                pd.Interval(wt_batching_interval[0], wt_batching_interval[1]))

            wt_unavailability_intervals = subtract_intervals(wt_unavailability_intervals, wt_batching_custom_interval)

        if wt_contention_pd_intervals:
            wt_contention_custom_intervals = pd_intervals_to_intervals(wt_contention_pd_intervals)
            wt_unavailability_intervals = subtract_intervals(wt_unavailability_intervals,
                                                             wt_contention_custom_intervals)

        if wt_prioritization_pd_intervals:
            wt_prioritization_custom_intervals = pd_intervals_to_intervals(wt_prioritization_pd_intervals)
            wt_unavailability_intervals = subtract_intervals(wt_unavailability_intervals,
                                                             wt_prioritization_custom_intervals)

        wt_unavailability = overall_duration(wt_unavailability_intervals)

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


def __duration_of_pd_intervals(intervals: List[pd.Interval]) -> pd.Timedelta:
    """Returns the duration of the intervals."""

    if len(intervals) == 0:
        return pd.Timedelta(0)

    result = pd.Timedelta(0)
    for interval in intervals:
        result += interval.right - interval.left

    return result

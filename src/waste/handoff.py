import concurrent.futures
import multiprocessing
from pathlib import Path
from typing import Dict, Optional

import click
import numpy as np
import pandas as pd

from . import core


def identify(log_path: Path, parallel_run=True) -> pd.DataFrame:
    click.echo(f'Parallel run: {parallel_run}')

    log = core.lifecycle_to_interval(log_path)
    parallel_activities = core.parallel_activities_with_heuristic_oracle(log)

    log_grouped = log.groupby(by='case:concept:name')
    all_handoffs = []
    if parallel_run:
        n_cores = multiprocessing.cpu_count() - 1
        handles = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=n_cores) as executor:
            for (case_id, case) in log_grouped:
                case = case.sort_values(by=['time:timestamp', 'start_timestamp'])
                handle = executor.submit(_identify_handoffs_per_case, case, parallel_activities)
                handles.append(handle)

        for h in handles:
            done = h.done()
            result = h.result()
            if done and not result.empty:
                all_handoffs.append(result)
    else:
        for (case_id, case) in log_grouped:
            case = case.sort_values(by=['time:timestamp', 'start_timestamp'])
            result = _identify_handoffs_per_case(case, parallel_activities)
            if result is not None:
                all_handoffs.append(result)

    result = _join_per_case_handoffs(all_handoffs)
    result['duration_sum_seconds'] = result['duration_sum'] / np.timedelta64(1, 's')
    return result


def _identify_ping_pongs_per_case(case: pd.DataFrame, parallel_activities: Dict[str, set]):
    activity_key = 'concept:name'
    resource_key = 'org:resource'
    start_time_key = 'start_timestamp'
    end_time_key = 'time:timestamp'

    case = case.sort_values(by=['time:timestamp', 'start_timestamp']).copy()
    case.reset_index()

    # NOTE: we need 4 consecutive events to identify the ping-pong pattern: A→B→C→D

    # tracking two events in past (e.g., in past: A, B, current: C, future: D)
    before_previous_event: Optional[pd.Series] = None
    previous_event: Optional[pd.Series] = None
    # tracking if A and C have the same activity and resource
    first_pair_same: bool = False
    # tracking if B and D have the same activity and resource
    second_pair_same: bool = False
    # collecting all ping-pongs
    ping_pongs = []
    for i in case.index:
        event = case.loc[i]  # current event C

        if previous_event is None:
            previous_event = event  # tracking the previous event B
            continue

        # parallelism identification
        parallel: bool = parallel_activities.get(event[activity_key], False)

        # handoff identification
        consecutive_timestamps = previous_event[end_time_key] <= event[start_time_key]
        activity_changed = previous_event[activity_key] != event[activity_key]
        resource_changed = previous_event[resource_key] != event[resource_key]
        handoff_occurred = False
        if consecutive_timestamps and activity_changed and resource_changed and not parallel:
            handoff_occurred = True

        if not handoff_occurred or before_previous_event is None:
            before_previous_event = previous_event  # tracking the event before previous A
            previous_event = event  # tracking the previous event B
            continue

        # evaluating pairs: A and C, B and D
        same_activity = before_previous_event[activity_key] == event[activity_key]
        same_resource = before_previous_event[resource_key] == event[resource_key]
        if same_activity and same_resource and not first_pair_same:
            first_pair_same = True
        elif same_activity and same_resource and first_pair_same:
            second_pair_same = True

        # ping-pong identification
        if first_pair_same and second_pair_same:
            ping_pongs.append({
                'source_activity': previous_event[activity_key],
                'source_resource': previous_event[resource_key],
                'destination_activity': event[activity_key],
                'destination_resource': event[resource_key]
            })
            first_pair_same = False
            second_pair_same = False

        before_previous_event = previous_event  # tracking the event before previous A
        previous_event = event  # tracking the previous event B

    return ping_pongs


def _identify_handoffs_per_case(case: pd.DataFrame, parallel_activities: Dict[str, set]):
    case = case.sort_values(by=['time:timestamp', 'start_timestamp']).copy()
    case.reset_index()

    next_events = case.shift(-1)
    resource_changed = case['org:resource'] != next_events['org:resource']
    activity_changed = case['concept:name'] != next_events['concept:name']
    consecutive_timestamps = case['time:timestamp'] <= next_events['start_timestamp']

    not_parallel = pd.Series(index=case.index)
    prev_activities = case['concept:name']
    next_activities = next_events['concept:name']
    for (i, pair) in enumerate(zip(prev_activities, next_activities)):
        if pair[0] == pair[1]:
            not_parallel.iat[i] = False
            continue
        parallel_set = parallel_activities.get(pair[1], None)
        if parallel_set and pair[0] in parallel_set:
            not_parallel.iat[i] = False
        else:
            not_parallel.iat[i] = True
    not_parallel = pd.Series(not_parallel)

    handoff_occurred = resource_changed & activity_changed & consecutive_timestamps & not_parallel
    handoffs_index = case[handoff_occurred].index

    # preparing a different dataframe for handoff reporting
    columns = ['source_activity', 'source_resource', 'destination_activity', 'destination_resource', 'duration']
    handoffs = pd.DataFrame(columns=columns)
    for loc in handoffs_index:
        source = case.loc[loc]
        destination = case.loc[loc + 1]

        # duration calculation
        destination_start = pd.to_datetime(destination['start_timestamp'], utc=True)
        source_end = pd.to_datetime(source['time:timestamp'], utc=True)
        duration = destination_start.tz_convert(tz='UTC') - source_end.tz_convert(tz='UTC')
        if duration < pd.Timedelta(0):
            duration = pd.Timedelta(0)

        # appending the handoff data
        handoffs = handoffs.append({
            'source_activity': source['concept:name'],
            'source_resource': source['org:resource'],
            'destination_activity': destination['concept:name'],
            'destination_resource': destination['org:resource'],
            'duration': duration
        }, ignore_index=True)

    # filling in N/A with some values
    handoffs['source_resource'] = handoffs['source_resource'].fillna('NA')
    handoffs['destination_resource'] = handoffs['destination_resource'].fillna('NA')

    # calculating frequency per case of the handoffs with the same activities and resources
    handoff_with_frequency = pd.DataFrame(columns=columns)
    handoff_grouped = handoffs.groupby(by=[
        'source_activity', 'source_resource', 'destination_activity', 'destination_resource'
    ])
    for group in handoff_grouped:
        pair, records = group
        handoff_with_frequency = handoff_with_frequency.append(pd.Series({
            'source_activity': pair[0],
            'source_resource': pair[1],
            'destination_activity': pair[2],
            'destination_resource': pair[3],
            'duration': records['duration'].sum(),
            'frequency': len(records)
        }), ignore_index=True)

    # dropping edge cases with Start and End as an activity
    starts_ends_values = ['Start', 'End']
    starts_and_ends = (handoff_with_frequency['source_activity'].isin(starts_ends_values)
                       & handoff_with_frequency['source_resource'].isin(starts_ends_values)) \
                      | (handoff_with_frequency['destination_activity'].isin(starts_ends_values)
                         & handoff_with_frequency['destination_resource'].isin(starts_ends_values))
    handoff_with_frequency = handoff_with_frequency[starts_and_ends == False]

    return handoff_with_frequency


def _join_per_case_handoffs(handoffs: list[pd.DataFrame]) -> pd.DataFrame:
    """Joins a list of handoffs summing up frequency and duration."""
    columns = ['source_activity', 'source_resource', 'destination_activity', 'destination_resource']
    grouped = pd.concat(handoffs).groupby(columns)
    result = pd.DataFrame(columns=columns)
    for pair_index, group in grouped:
        source_activity, source_resource, destination_activity, destination_resource = pair_index
        group_duration: pd.Timedelta = group['duration'].sum()
        group_frequency: float = group['frequency'].sum()
        result = result.append({
            'source_activity': source_activity,
            'source_resource': source_resource,
            'destination_activity': destination_activity,
            'destination_resource': destination_resource,
            'duration_sum': group_duration,
            'frequency': group_frequency
        }, ignore_index=True)
    result.reset_index(drop=True, inplace=True)
    return result

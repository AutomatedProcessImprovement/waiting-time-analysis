from pathlib import Path
from typing import Optional, Dict

import click
import pandas as pd

from ..core import core


def identify(log_path: Path, parallel_run=True) -> Optional[pd.DataFrame]:
    click.echo(f'Parallel run: {parallel_run}')
    result = core.identify_main(
        log_path=log_path,
        identify_fn_per_case=_identify_ping_pongs_per_case,
        join_fn=_join_per_case_ping_pongs,
        parallel_run=parallel_run)
    return result


def _is_parallel(activity_name_one: str, activity_name_two: str, parallel_activities: Dict[str, set]) -> bool:
    activity_names = parallel_activities.get(activity_name_one, None)
    if activity_names and activity_name_two in activity_names:
        return True
    return False


def _identify_ping_pongs_per_case(case: pd.DataFrame, parallel_activities: Dict[str, set]) -> pd.DataFrame:
    activity_key = 'concept:name'
    resource_key = 'org:resource'
    start_time_key = 'start_timestamp'
    end_time_key = 'time:timestamp'
    time_format = '%Y-%m-%d %H:%M:%S%z'

    case = case.sort_values(by=['time:timestamp', 'start_timestamp']).copy()
    case.reset_index()

    # NOTE: we need 4 consecutive events to identify the ping-pong pattern: A→B→C→D

    # tracking two events in past (e.g., in past: A, B, current: C, future: D)
    pre_pre_previous_event: Optional[pd.Series] = None
    pre_previous_event: Optional[pd.Series] = None
    previous_event: Optional[pd.Series] = None

    # collecting all ping-pongs
    ping_pongs = {}

    for i in case.index:
        event = case.loc[i]  # current event C

        if previous_event is None:
            previous_event = event  # tracking the previous event B
            continue

        if pre_previous_event is None:
            pre_previous_event = previous_event
            previous_event = event
            continue

        if pre_pre_previous_event is None:
            pre_pre_previous_event = pre_previous_event
            pre_previous_event = previous_event
            previous_event = event
            continue

        # now all 4 events are populated

        # skipping parallel events
        parallel: bool = _is_parallel(pre_pre_previous_event[activity_key], pre_previous_event[activity_key],
                                      parallel_activities) or \
                         _is_parallel(pre_previous_event[activity_key], previous_event[activity_key],
                                      parallel_activities) or \
                         _is_parallel(previous_event[activity_key], event[activity_key], parallel_activities)

        consecutive_timestamps: bool = pre_pre_previous_event[end_time_key] <= pre_previous_event[start_time_key] and \
                                       pre_previous_event[end_time_key] <= previous_event[start_time_key] and \
                                       previous_event[end_time_key] <= event[start_time_key]

        activities_match: bool = pre_pre_previous_event[activity_key] == previous_event[activity_key] and \
                                 pre_previous_event[activity_key] == event[activity_key]

        resources_match: bool = pre_pre_previous_event[resource_key] == previous_event[resource_key] and \
                                pre_previous_event[resource_key] == event[resource_key]

        if consecutive_timestamps and activities_match and resources_match and not parallel:
            ping_pong_key = f"{previous_event[activity_key]}:{previous_event[resource_key]}:{event[activity_key]}:{event[resource_key]}"

            # converting timestamps' strings to pd.Timestamp
            if isinstance(previous_event[start_time_key], str):
                previous_event[start_time_key] = pd.to_datetime(previous_event[start_time_key])
            if isinstance(pre_previous_event[end_time_key], str):
                pre_previous_event[end_time_key] = pd.to_datetime(pre_previous_event[end_time_key])
            if isinstance(event[start_time_key], str):
                event[start_time_key] = pd.to_datetime(event[start_time_key])
            if isinstance(previous_event[end_time_key], str):
                previous_event[end_time_key] = pd.to_datetime(previous_event[end_time_key])

            step2_handoff_duration = \
                previous_event[start_time_key].tz_convert(tz='UTC') - \
                pre_previous_event[end_time_key].tz_convert(tz='UTC')
            step3_handoff_duration = \
                event[start_time_key].tz_convert(tz='UTC') - \
                previous_event[end_time_key].tz_convert(tz='UTC')
            ping_pong = {
                'source_activity': previous_event[activity_key],
                'source_resource': previous_event[resource_key],
                'destination_activity': event[activity_key],
                'destination_resource': event[resource_key],
                'frequency': 1,
                'duration': step2_handoff_duration + step3_handoff_duration
            }
            if ping_pong_key in ping_pongs:
                ping_pong['frequency'] = ping_pongs[ping_pong_key]['frequency'] + 1
                ping_pong['duration'] = ping_pongs[ping_pong_key]['duration'] + ping_pong['duration']
            ping_pongs[ping_pong_key] = ping_pong

        pre_pre_previous_event = pre_previous_event
        pre_previous_event = previous_event
        previous_event = event

    return pd.DataFrame(ping_pongs.values())


def _join_per_case_ping_pongs(items: list[pd.DataFrame]) -> pd.DataFrame:
    """Joins a list of ping pongs summing up frequency and duration."""
    columns = ['source_activity', 'source_resource', 'destination_activity', 'destination_resource']
    grouped = pd.concat(items).groupby(columns)
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

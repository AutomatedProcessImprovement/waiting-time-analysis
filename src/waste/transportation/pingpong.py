from typing import Optional, Dict

import click
import pandas as pd
from waste.core import core


def identify(log: pd.DataFrame, parallel_activities: dict[str, set], parallel_run=True) -> Optional[pd.DataFrame]:
    click.echo(f'Ping-pong identification. Parallel run: {parallel_run}')
    result = core.identify_main(
        log=log,
        parallel_activities=parallel_activities,
        identify_fn_per_case=_identify_ping_pongs_per_case,
        join_fn=core.join_per_case_items,
        parallel_run=parallel_run)
    return result


def _is_parallel(activity_name_one: str, activity_name_two: str, parallel_activities: Dict[str, set]) -> bool:
    activity_names = parallel_activities.get(activity_name_one, None)
    if activity_names and activity_name_two in activity_names:
        return True
    return False


def _identify_ping_pongs_per_case(case: pd.DataFrame, parallel_activities: Dict[str, set],
                                  case_id: str) -> pd.DataFrame:
    case = case.sort_values(by=[core.END_TIMESTAMP_KEY, core.START_TIMESTAMP_KEY]).copy()
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
        parallel: bool = _is_parallel(pre_pre_previous_event[core.ACTIVITY_KEY], pre_previous_event[core.ACTIVITY_KEY],
                                      parallel_activities) or \
                         _is_parallel(pre_previous_event[core.ACTIVITY_KEY], previous_event[core.ACTIVITY_KEY],
                                      parallel_activities) or \
                         _is_parallel(previous_event[core.ACTIVITY_KEY], event[core.ACTIVITY_KEY], parallel_activities)

        consecutive_timestamps: bool = \
            pre_pre_previous_event[core.END_TIMESTAMP_KEY] <= pre_previous_event[core.START_TIMESTAMP_KEY] and \
            pre_previous_event[core.END_TIMESTAMP_KEY] <= previous_event[core.START_TIMESTAMP_KEY] and \
            previous_event[core.END_TIMESTAMP_KEY] <= event[core.START_TIMESTAMP_KEY]

        activities_match: bool = pre_pre_previous_event[core.ACTIVITY_KEY] == previous_event[core.ACTIVITY_KEY] and \
                                 pre_previous_event[core.ACTIVITY_KEY] == event[core.ACTIVITY_KEY]

        resources_match: bool = pre_pre_previous_event[core.RESOURCE_KEY] == previous_event[core.RESOURCE_KEY] and \
                                pre_previous_event[core.RESOURCE_KEY] == event[core.RESOURCE_KEY]

        if consecutive_timestamps and activities_match and resources_match and not parallel:
            ping_pong_key = f"{previous_event[core.ACTIVITY_KEY]}:{previous_event[core.RESOURCE_KEY]}:{event[core.ACTIVITY_KEY]}:{event[core.RESOURCE_KEY]}"

            # converting timestamps' strings to pd.Timestamp
            if isinstance(previous_event[core.START_TIMESTAMP_KEY], str):
                previous_event[core.START_TIMESTAMP_KEY] = pd.to_datetime(previous_event[core.START_TIMESTAMP_KEY])
            if isinstance(pre_previous_event[core.END_TIMESTAMP_KEY], str):
                pre_previous_event[core.END_TIMESTAMP_KEY] = pd.to_datetime(pre_previous_event[core.END_TIMESTAMP_KEY])
            if isinstance(event[core.START_TIMESTAMP_KEY], str):
                event[core.START_TIMESTAMP_KEY] = pd.to_datetime(event[core.START_TIMESTAMP_KEY])
            if isinstance(previous_event[core.END_TIMESTAMP_KEY], str):
                previous_event[core.END_TIMESTAMP_KEY] = pd.to_datetime(previous_event[core.END_TIMESTAMP_KEY])

            step2_handoff_duration = \
                previous_event[core.START_TIMESTAMP_KEY].tz_convert(tz='UTC') - \
                pre_previous_event[core.END_TIMESTAMP_KEY].tz_convert(tz='UTC')
            step3_handoff_duration = \
                event[core.START_TIMESTAMP_KEY].tz_convert(tz='UTC') - \
                previous_event[core.END_TIMESTAMP_KEY].tz_convert(tz='UTC')
            ping_pong = {
                'source_activity': previous_event[core.ACTIVITY_KEY],
                'source_resource': previous_event[core.RESOURCE_KEY],
                'destination_activity': event[core.ACTIVITY_KEY],
                'destination_resource': event[core.RESOURCE_KEY],
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

    df = pd.DataFrame(ping_pongs.values())
    df['case_id'] = case_id

    return df

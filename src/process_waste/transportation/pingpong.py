from typing import Optional, Dict

import click
import pandas as pd

from batch_processing_analysis.config import EventLogIDs
from process_waste import default_log_ids
from process_waste.core import core


def identify(
        log: pd.DataFrame,
        parallel_activities: Dict[str, set],
        parallel_run=True,
        log_ids: Optional[EventLogIDs] = None) -> Optional[pd.DataFrame]:
    click.echo(f'Ping-pong identification. Parallel run: {parallel_run}')
    result = core.identify_main(
        log=log,
        parallel_activities=parallel_activities,
        identify_fn_per_case=_identify_ping_pongs_per_case,
        join_fn=core.join_per_case_items,
        parallel_run=parallel_run,
        log_ids=log_ids)
    return result


def _is_parallel(activity_name_one: str, activity_name_two: str, parallel_activities: Dict[str, set]) -> bool:
    activity_names = parallel_activities.get(activity_name_one, None)
    if activity_names and activity_name_two in activity_names:
        return True
    return False


def _identify_ping_pongs_per_case(case: pd.DataFrame, **kwargs) -> pd.DataFrame:
    parallel_activities = kwargs['parallel_activities']
    case_id = kwargs['case_id']
    enabled_on = kwargs['enabled_on']
    log_ids = kwargs['log_ids']

    if not log_ids:
        log_ids = default_log_ids

    case = case.sort_values(by=[log_ids.end_time, log_ids.start_time]).copy()
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
        parallel: bool = _is_parallel(pre_pre_previous_event[log_ids.activity], pre_previous_event[log_ids.activity],
                                      parallel_activities) or \
                         _is_parallel(pre_previous_event[log_ids.activity], previous_event[log_ids.activity],
                                      parallel_activities) or \
                         _is_parallel(previous_event[log_ids.activity], event[log_ids.activity], parallel_activities)

        consecutive_timestamps: bool = \
            pre_pre_previous_event[log_ids.end_time] <= pre_previous_event[log_ids.start_time] and \
            pre_previous_event[log_ids.end_time] <= previous_event[log_ids.start_time] and \
            previous_event[log_ids.end_time] <= event[log_ids.start_time]

        activities_match: bool = pre_pre_previous_event[log_ids.activity] == previous_event[log_ids.activity] and \
                                 pre_previous_event[log_ids.activity] == event[log_ids.activity]

        resources_match: bool = pre_pre_previous_event[log_ids.resource] == previous_event[log_ids.resource] and \
                                pre_previous_event[log_ids.resource] == event[log_ids.resource]

        if consecutive_timestamps and activities_match and resources_match and not parallel:
            ping_pong_key = f"{previous_event[log_ids.activity]}:{previous_event[log_ids.resource]}:{event[log_ids.activity]}:{event[log_ids.resource]}"
            if enabled_on:
                step2_handoff_duration = \
                    previous_event[log_ids.start_time] - previous_event[log_ids.enabled_time]
                step3_handoff_duration = \
                    event[log_ids.start_time] - event[log_ids.enabled_time]
            else:
                step2_handoff_duration = \
                    previous_event[log_ids.start_time] - pre_previous_event[log_ids.end_time]
                step3_handoff_duration = \
                    event[log_ids.start_time] - previous_event[log_ids.end_time]
            ping_pong = {
                'source_activity': previous_event[log_ids.activity],
                'source_resource': previous_event[log_ids.resource],
                'destination_activity': event[log_ids.activity],
                'destination_resource': event[log_ids.resource],
                'frequency': 1,
                'wt_total': step2_handoff_duration + step3_handoff_duration
            }
            if ping_pong_key in ping_pongs:
                ping_pong['frequency'] = ping_pongs[ping_pong_key]['frequency'] + 1
                ping_pong['wt_total'] = ping_pongs[ping_pong_key]['wt_total'] + ping_pong['wt_total']
            ping_pongs[ping_pong_key] = ping_pong

        pre_pre_previous_event = pre_previous_event
        pre_previous_event = previous_event
        previous_event = event

    df = pd.DataFrame(ping_pongs.values())
    df['case_id'] = case_id

    return df

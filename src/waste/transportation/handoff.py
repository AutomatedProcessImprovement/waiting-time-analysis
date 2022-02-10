from pathlib import Path
from typing import Dict

import click
import pandas as pd

from waste import core


def identify(log: pd.DataFrame, parallel_activities: dict[str, set], parallel_run=True) -> pd.DataFrame:
    click.echo(f'Handoff identification. Parallel run: {parallel_run}')
    result = core.identify_main(
        log=log,
        parallel_activities=parallel_activities,
        identify_fn_per_case=_identify_handoffs_per_case,
        join_fn=core.join_per_case_items,
        parallel_run=parallel_run)
    return result


def _identify_handoffs_per_case(case: pd.DataFrame, parallel_activities: Dict[str, set], case_id: str):
    case = case.sort_values(by=[core.END_TIMESTAMP_KEY, core.START_TIMESTAMP_KEY]).copy()
    case.reset_index()

    next_events = case.shift(-1)
    resource_changed = case[core.RESOURCE_KEY] != next_events[core.RESOURCE_KEY]
    activity_changed = case[core.ACTIVITY_KEY] != next_events[core.ACTIVITY_KEY]
    consecutive_timestamps = case[core.END_TIMESTAMP_KEY] <= next_events[core.START_TIMESTAMP_KEY]

    not_parallel = pd.Series(index=case.index)
    prev_activities = case[core.ACTIVITY_KEY]
    next_activities = next_events[core.ACTIVITY_KEY]
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
        destination_start = destination[core.START_TIMESTAMP_KEY]
        source_end = destination[core.ENABLED_TIMESTAMP_KEY]
        duration = destination_start.tz_convert(tz='UTC') - source_end.tz_convert(tz='UTC')
        if duration < pd.Timedelta(0):
            duration = pd.Timedelta(0)

        # appending the handoff data
        handoffs = handoffs.append({  # TODO: change to pd.concat
            'source_activity': source[core.ACTIVITY_KEY],
            'source_resource': source[core.RESOURCE_KEY],
            'destination_activity': destination[core.ACTIVITY_KEY],
            'destination_resource': destination[core.RESOURCE_KEY],
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
        handoff_with_frequency = handoff_with_frequency.append(pd.Series({  # TODO: change to pd.concat
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

    handoff_with_frequency['case_id'] = case_id

    return handoff_with_frequency

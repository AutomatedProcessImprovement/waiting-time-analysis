import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Tuple, Optional, Union

import numpy as np
import pandas as pd

from . import core


def identify(log_path: Path) -> pd.DataFrame:
    log = core.lifecycle_to_interval(log_path)
    log_grouped = log.groupby(by='case:concept:name')
    all_handoffs = []
    parallel_activities = core.parallel_activities_with_alpha_oracle(log)
    for (case_id, case) in log_grouped:
        case = case.sort_values(by='time:timestamp')
        handoffs = identify_handoffs(case, parallel_activities)
        if handoffs is not None:
            all_handoffs.append(handoffs)
    result = join_handoffs(all_handoffs)
    result['duration_sum_seconds'] = result['duration_sum'] / np.timedelta64(1, 's')
    return result


@dataclass
class ParallelEventsPair:
    events: pd.DataFrame  # original data
    id: str = field(init=False)
    alias: pd.DataFrame = field(init=False)  # new temporal representation

    def __post_init__(self):
        self.id = str(uuid.uuid4())
        self.alias = self.compose_alias(self.events)

    @staticmethod
    def compose_alias(events) -> pd.DataFrame:
        if len(events) == 0:
            raise ValueError('Events must not be of length 0')
        replacement = events.iloc[0].copy()
        replacement['concept:name'] = ':'.join(events['concept:name'])
        replacement['start_timestamp'] = events['start_timestamp'].min()
        replacement['time:timestamp'] = events['time:timestamp'].max()
        replacement['org:resource'] = ':'.join(events['org:resource'])
        return replacement


def make_aliases_for_concurrent_activities(
        case: pd.DataFrame,
        activities: list[Tuple]) -> dict[str, ParallelEventsPair]:
    aliases: dict[str, ParallelEventsPair] = {}

    for names in activities:
        if len(names) == 0:
            continue

        # extract concurrent activities data
        data = pd.DataFrame()
        for name in names:
            event = case[case['concept:name'] == name]
            data = data.append(event)

        if data.size == 0:
            continue

        alias = ParallelEventsPair(events=data)
        aliases[alias.id] = alias

    return aliases


def replace_concurrent_activities_with_aliases(
        case: pd.DataFrame,
        activities: list[Tuple],
        aliases: dict[str, ParallelEventsPair]) -> pd.DataFrame:
    """Creating a new case dataframe with concurrent activities replaced by aliases to identify all sequential handoffs."""

    # dropping concurrent activities
    case_with_aliases = case.copy()
    for names in activities:
        indices = case_with_aliases[case_with_aliases['concept:name'].isin(names)].index
        case_with_aliases.drop(index=indices, inplace=True)

    # adding concurrent activities replacements
    for alias_id in aliases:
        case_with_aliases = case_with_aliases.append(aliases[alias_id].alias)

    case_with_aliases.reset_index(inplace=True, drop=True)
    case_with_aliases.sort_values(by='start_timestamp', inplace=True)
    return case_with_aliases


def identify_sequential_handoffs_locations(case: pd.DataFrame) -> Union[bool, int]:
    resource_changed = case['org:resource'] != case.shift(-1)['org:resource']
    activity_changed = case['concept:name'] != case.shift(-1)['concept:name']
    handoff_occurred = resource_changed & activity_changed  # both conditions must be satisfied
    return handoff_occurred


def identify_sequential_handoffs(case: pd.DataFrame) -> pd.DataFrame:
    case = case.sort_values(by=['start_timestamp', 'time:timestamp'])

    handoff_occurred = identify_sequential_handoffs_locations(case)

    # preparing a different dataframe for handoff reporting
    handoff = pd.DataFrame(
        columns=['source_activity', 'source_resource', 'destination_activity', 'destination_resource', 'duration'])
    handoff.loc[:, 'source_activity'] = case[handoff_occurred]['concept:name']
    handoff.loc[:, 'source_resource'] = case[handoff_occurred]['org:resource']
    handoff.loc[:, 'destination_activity'] = case[handoff_occurred].shift(-1)['concept:name']
    handoff.loc[:, 'destination_resource'] = case[handoff_occurred].shift(-1)['org:resource']
    # handoff['duration'] = case[handoff_occurred].shift(-1)['start_timestamp'] - case[handoff_occurred]['time:timestamp']
    handoff['duration'] = case[handoff_occurred].shift(-1)['start_timestamp'] - case[handoff_occurred][
        'time:timestamp']  # TODO: enabled_timestamp instead of start_timestamp?

    # NOTE: dealing with negative durations in cases where the next activity started before the previous one ended
    duration = handoff['duration'] / np.timedelta64(1, 's')
    handoff.at[duration < 0] = pd.Timedelta(0)

    # dropping an event at the end which is always 'True'
    handoff.reset_index(drop=True, inplace=True)
    handoff.drop(handoff.tail(1).index, inplace=True)

    # filling in N/A with some values
    handoff['source_resource'] = handoff['source_resource'].fillna('NA')
    handoff['destination_resource'] = handoff['destination_resource'].fillna('NA')

    # calculating frequency of the handoffs with the same activities and resources
    handoff_with_frequency = pd.DataFrame(
        columns=['source_activity', 'source_resource', 'destination_activity', 'destination_resource', 'duration'])
    handoff_grouped = handoff.groupby(
        by=['source_activity', 'source_resource', 'destination_activity', 'destination_resource'])
    for group in handoff_grouped:
        pair, records = group
        # print(f"Source-destination pair: {pair}, frequency per case: {len(records)}")
        handoff_with_frequency = handoff_with_frequency.append(pd.Series({
            'source_activity': pair[0],
            'source_resource': pair[1],
            'destination_activity': pair[2],
            'destination_resource': pair[3],
            'duration': records['duration'].sum(),
            'frequency': len(records)
        }), ignore_index=True)

    return handoff_with_frequency


# NOTE: mutates aliases
def identify_concurrent_handoffs(case: pd.DataFrame, aliases: dict[str, ParallelEventsPair]) -> Optional[pd.DataFrame]:
    # Coming back to concurrent activities to identify sequential to concurrent and concurrent to sequential handoffs.
    case = case.sort_values(by=['start_timestamp', 'time:timestamp'])

    # TODO: does this really give us previous and next?
    def _get_previous_and_next_events(case: pd.DataFrame, index: int) -> (
            Optional[pd.DataFrame], Optional[pd.DataFrame]):
        previous_event = None
        next_event = None
        if index != 0:
            previous_event = case.iloc[index - 1]
        if index != len(case) - 1:
            next_event = case.iloc[index + 1]
        return previous_event, next_event

    if len(aliases) == 0:
        return None

    handoff_occurred = identify_sequential_handoffs_locations(case)

    potential_handoffs = case[handoff_occurred].copy()
    potential_handoffs.sort_values(by=['start_timestamp', 'time:timestamp'], inplace=True)
    potential_handoffs.reset_index(inplace=True, drop=True)

    if potential_handoffs.size == 0:
        return None

    concurrent_handoffs = []
    for alias_id in aliases:
        index = potential_handoffs[potential_handoffs['concept:name'] == aliases[alias_id].alias['concept:name']].index
        if index.size == 0:
            continue
        previous_event, next_event = _get_previous_and_next_events(potential_handoffs, index[0])
        prev_handoffs = identify_concurrent_handoffs_left(previous_event, aliases[alias_id].events)
        next_handoffs = identify_concurrent_handoffs_right(next_event, aliases[alias_id].events)
        if len(prev_handoffs) > 0:
            concurrent_handoffs.append(prev_handoffs)
        if len(next_handoffs) > 0:
            concurrent_handoffs.append(next_handoffs)

    if len(concurrent_handoffs) > 0:
        return pd.concat(concurrent_handoffs)
    return None


def identify_concurrent_handoffs_left(previous_event: pd.Series, concurrent_events: pd.DataFrame):
    all_handoffs = []
    for i in concurrent_events.index:
        sequence = pd.DataFrame([previous_event, concurrent_events.loc[i]])
        handoffs = identify_sequential_handoffs(sequence)
        all_handoffs.append(handoffs)
    return pd.concat(all_handoffs)


def identify_concurrent_handoffs_right(next_event: Optional[pd.Series], concurrent_events: pd.DataFrame):
    if next_event is None:
        return []

    all_handoffs = []
    for i in concurrent_events.index:
        sequence = pd.DataFrame([concurrent_events.loc[i], next_event])
        handoffs = identify_sequential_handoffs(sequence)
        all_handoffs.append(handoffs)
    return pd.concat(all_handoffs)


def identify_handoffs(case: pd.DataFrame, parallel_activities: list[tuple] = None) -> Optional[pd.DataFrame]:
    # TODO: case = core.add_enabled_timestamps(case)
    if parallel_activities is None:
        parallel_activities = core.get_concurrent_activities(case)  # NOTE: per case concurrency identification
    aliases = make_aliases_for_concurrent_activities(case, parallel_activities)
    case_with_aliases = replace_concurrent_activities_with_aliases(case, parallel_activities, aliases)
    sequential_handoffs = identify_sequential_handoffs(case_with_aliases)
    concurrent_handoffs = identify_concurrent_handoffs(case_with_aliases, aliases)

    # removing handoffs related to aliases
    for alias_id in aliases:
        index = sequential_handoffs[sequential_handoffs['source_activity'] == alias_id].index
        sequential_handoffs.drop(index=index, inplace=True)
        index = sequential_handoffs[sequential_handoffs['destination_activity'] == alias_id].index
        sequential_handoffs.drop(index=index, inplace=True)

    handoffs = []
    if sequential_handoffs is not None and not sequential_handoffs.empty:
        handoffs.append(sequential_handoffs)
    if concurrent_handoffs is not None and not concurrent_handoffs.empty:
        handoffs.append(concurrent_handoffs)
    if len(handoffs) > 1:
        return pd.concat([sequential_handoffs, concurrent_handoffs])  # TODO: reset index
    elif len(handoffs) == 1:
        return handoffs[0]
    return None


def join_handoffs(handoffs: list[pd.DataFrame]) -> pd.DataFrame:
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

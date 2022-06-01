import operator
from functools import reduce
from typing import Dict, Optional, Tuple

import click
import pandas as pd

from batch_processing_analysis.config import EventLogIDs
from process_waste import core, WAITING_TIME_TOTAL_KEY, WAITING_TIME_BATCHING_KEY, WAITING_TIME_CONTENTION_KEY, \
    WAITING_TIME_PRIORITIZATION_KEY, WAITING_TIME_UNAVAILABILITY_KEY, WAITING_TIME_EXTRANEOUS_KEY, default_log_ids, \
    convert_timestamp_columns_to_datetime
from process_waste.waiting_time.prioritization_and_contention import detect_prioritization_or_contention
from process_waste.waiting_time.resource_unavailability import detect_waiting_time_due_to_unavailability


def identify(
        log: pd.DataFrame,
        parallel_activities: Dict[str, set],
        parallel_run=True,
        log_ids: Optional[EventLogIDs] = None) -> pd.DataFrame:
    click.echo(f'Handoff identification. Parallel run: {parallel_run}')
    result = core.identify_main(
        log=log,
        parallel_activities=parallel_activities,
        identify_fn_per_case=__identify_handoffs_per_case_and_make_report,
        join_fn=core.join_per_case_items,
        parallel_run=parallel_run,
        log_ids=log_ids)
    return result


def __identify_handoffs_per_case_and_make_report(case: pd.DataFrame, **kwargs) -> pd.DataFrame:
    parallel_activities = kwargs['parallel_activities']
    case_id = kwargs['case_id']
    log_calendar = kwargs['log_calendar']
    log = kwargs['log']
    log_ids = kwargs.get('log_ids')

    if not log_ids:
        log_ids = default_log_ids

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
    if not log_ids:
        log_ids = default_log_ids

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

        wt_total = destination[WAITING_TIME_TOTAL_KEY]
        wt_batching = __wt_batching(destination)
        wt_contention, wt_prioritization = __wt_contention_and_prioritization(destination_index, log, log_ids)
        wt_unavailability = __wt_unavailability(destination_index, log, log_calendar, log_ids)
        wt_extraneous = __wt_extraneous(wt_total, wt_batching, wt_contention, wt_prioritization, wt_unavailability)

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


def __wt_extraneous(waiting_time, waiting_time_batch, waiting_time_contention, waiting_time_prioritization,
                    waiting_time_unavailability) -> pd.Timedelta:
    """Calculates the extraneous waiting time."""

    waiting_time_extraneous = reduce(operator.__sub__, [
        waiting_time,
        waiting_time_batch,
        waiting_time_prioritization,
        waiting_time_contention,
        waiting_time_unavailability
    ])
    if pd.isna(waiting_time_extraneous):
        waiting_time_extraneous = pd.Timedelta(0)
    return waiting_time_extraneous


def __wt_unavailability(destination_index, log, log_calendar, log_ids) -> pd.Timedelta:
    """Discovers waiting time due to unavailability of resources."""

    detect_waiting_time_due_to_unavailability(destination_index, log, log_calendar, log_ids=log_ids)
    waiting_time_unavailability = log.loc[destination_index][WAITING_TIME_UNAVAILABILITY_KEY].values.sum()
    if pd.isna(waiting_time_unavailability):
        waiting_time_unavailability = pd.Timedelta(0)
    return waiting_time_unavailability


def __wt_contention_and_prioritization(
        destination_index: pd.Index,
        log: pd.DataFrame,
        log_ids: EventLogIDs) -> Tuple[pd.Timedelta, pd.Timedelta]:
    """Discovers waiting time due to resource contention and prioritization."""

    # Resource contention and prioritization
    # waiting time due to contention and prioritization: we take the WT of the destination activity only,
    # because the WT of the source activity isn't related to this handoff
    detect_prioritization_or_contention(destination_index, log, log_ids=log_ids)
    waiting_time_prioritization = log.loc[destination_index][WAITING_TIME_PRIORITIZATION_KEY].values.sum()
    if pd.isna(waiting_time_prioritization):
        waiting_time_prioritization = pd.Timedelta(0)
    waiting_time_contention = log.loc[destination_index][WAITING_TIME_CONTENTION_KEY].values.sum()
    if pd.isna(waiting_time_contention):
        waiting_time_contention = pd.Timedelta(0)
    return waiting_time_contention, waiting_time_prioritization


def __wt_batching(destination: pd.DataFrame) -> pd.Timedelta:
    """Discovers waiting time due to batching."""

    # waiting time due to batching: we take the WT of the destination activity only,
    # because the WT of the source activity isn't related to this handoff
    batch_column_key = 'batch_creation_wt'

    waiting_time_batch = pd.Timedelta(0)
    if batch_column_key in destination.index:
        waiting_time_batch = destination[batch_column_key]
    if pd.isna(waiting_time_batch):
        waiting_time_batch = pd.Timedelta(0)
    return waiting_time_batch


def __mark_strict_handoffs(
        case: pd.DataFrame,
        parallel_activities: Optional[Dict[str, set]] = None,
        log_ids: Optional[EventLogIDs] = None) -> pd.DataFrame:
    if not log_ids:
        log_ids = default_log_ids

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
    if not log_ids:
        log_ids = default_log_ids

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

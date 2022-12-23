import concurrent.futures
import multiprocessing
from typing import Dict, Optional, List

import click
import numpy as np
import pandas as pd
from tqdm import tqdm

from wta import GRANULARITY_MINUTES
from wta.helpers import print_section_boundaries, convert_timestamp_columns_to_datetime, log_ids_non_nil, \
    EventLogIDs, TRANSITION_COLUMN_KEY
from wta.waiting_time import analysis as wt_analysis


@print_section_boundaries('Activity Transitions Analysis')
def identify(
        log: pd.DataFrame,
        parallel_activities: Dict[str, set],
        parallel_run=True,
        log_ids: Optional[EventLogIDs] = None,
        calendar: Optional[Dict] = None,
        group_results: bool = True) -> Optional[pd.DataFrame]:
    from wta.calendars.calendars import make as make_calendar

    click.echo(f'Parallel run: {parallel_run}')

    log_ids = log_ids_non_nil(log_ids)

    if not calendar:
        log_calendar = make_calendar(log, granularity=GRANULARITY_MINUTES, log_ids=log_ids)
    else:
        log_calendar = calendar

    if parallel_run:
        all_items = __multiprocess_run(log, log_ids, log_calendar, parallel_activities)
    else:
        all_items = __sequential_run(log, log_ids, log_calendar, parallel_activities)

    if len(all_items) == 0:
        return None

    if group_results:
        result = __join_per_case_items(all_items, log_ids=log_ids)
        if result is not None:
            result['wt_total_seconds'] = result[log_ids.wt_total] / np.timedelta64(1, 's')
    else:
        result = __create_single_dataframe(all_items)

    return result


def __sequential_run(log, log_ids, calendar, parallel_activities):
    results = []
    log_grouped = log.groupby(by=log_ids.case)

    for (case_id, case) in log_grouped:
        case = case.sort_values(by=[log_ids.end_time, log_ids.start_time])
        result = __identify_transitions_per_case_and_make_report(
            case,
            parallel_activities=parallel_activities,
            case_id=case_id,
            log_calendar=calendar,
            log=log,
            log_ids=log_ids)

        if result is not None:
            results.append(result)

    return results


def __multiprocess_run(log, log_ids, calendar, parallel_activities):
    all_items = []
    n_cores = multiprocessing.cpu_count() - 1
    handles = []
    log_grouped = log.groupby(by=log_ids.case)

    with concurrent.futures.ProcessPoolExecutor(max_workers=n_cores) as executor:
        for (case_id, case) in tqdm(log_grouped, desc='Submitting tasks for concurrent execution'):
            case = case.sort_values(by=[log_ids.end_time, log_ids.start_time])
            handle = executor.submit(__identify_transitions_per_case_and_make_report,
                                     case,
                                     parallel_activities=parallel_activities,
                                     case_id=case_id,
                                     log_calendar=calendar,
                                     log=log,
                                     log_ids=log_ids)
            handles.append(handle)

    for h in tqdm(handles, desc='Waiting for tasks to finish'):
        done = h.done()
        result = h.result()
        if done and not result.empty:
            all_items.append(result)

    return all_items


def __identify_transitions_per_case_and_make_report(case: pd.DataFrame, **kwargs) -> pd.DataFrame:
    parallel_activities = kwargs['parallel_activities']
    case_id = kwargs['case_id']
    log_calendar = kwargs['log_calendar']
    log = kwargs['log']
    log_ids = log_ids_non_nil(kwargs.get('log_ids'))

    case = case.sort_values(by=[log_ids.end_time, log_ids.start_time]).copy()

    # converting timestamps to datetime
    log = convert_timestamp_columns_to_datetime(log, log_ids)
    case = convert_timestamp_columns_to_datetime(case, log_ids)

    __mark_activity_transitions(case, parallel_activities, log_ids=log_ids)

    transitions = wt_analysis.run(case, log_calendar, log, log_ids=log_ids)

    transitions_with_frequency = __calculate_frequency_and_duration(transitions, log_ids=log_ids)

    # dropping edge cases with Start and End as an activity
    starts_ends_values = ['Start', 'End']
    starts_and_ends = (transitions_with_frequency['source_activity'].isin(starts_ends_values)
                       & transitions_with_frequency['source_resource'].isin(starts_ends_values)) \
                      | (transitions_with_frequency['destination_activity'].isin(starts_ends_values)
                         & transitions_with_frequency['destination_resource'].isin(starts_ends_values))
    transitions_with_frequency = transitions_with_frequency[starts_and_ends == False]

    # attaching case ID as additional information
    transitions_with_frequency['case_id'] = case_id

    return transitions_with_frequency


def __mark_activity_transitions(
        case: pd.DataFrame,
        parallel_activities: Optional[Dict[str, set]] = None,
        log_ids: Optional[EventLogIDs] = None):
    log_ids = log_ids_non_nil(log_ids)

    # NOTE: we assume (a) the case was sorted by end time

    if not parallel_activities:
        parallel_activities = {}

    case[log_ids.transition_source_index] = np.NAN

    # processing the case backwards
    reversed_index = list(reversed(case.index))
    for i in range(len(reversed_index)):
        non_concurrent_previous_event_found = False

        index = reversed_index[i]
        current_event = case.loc[index]
        parallel_activities_for_current_event = parallel_activities.get(current_event[log_ids.activity], [])

        previous_event_index_delta = i + 1
        while not non_concurrent_previous_event_found:
            if previous_event_index_delta > len(reversed_index) - 1:
                break

            previous_event_index = reversed_index[previous_event_index_delta]
            previous_event = case.loc[previous_event_index]
            # Check if they are overlapping
            overlapping_activity_instances = previous_event[log_ids.end_time] > current_event[log_ids.start_time]
            if previous_event[
                log_ids.activity] in parallel_activities_for_current_event or overlapping_activity_instances:
                # If they are concurrent activities, or overlapping instances, jump to the previous event
                previous_event_index_delta += 1
            else:
                # If they are not concurrent nor overlapping, transition!
                case.at[index, TRANSITION_COLUMN_KEY] = previous_event_index
                non_concurrent_previous_event_found = True


def __calculate_frequency_and_duration(transitions: pd.DataFrame,
                                       log_ids: Optional[EventLogIDs] = None) -> pd.DataFrame:
    log_ids = log_ids_non_nil(log_ids)

    # calculating frequency per case of the transitions with the same activities and resources
    columns = transitions.columns.tolist()
    transition_with_frequency = pd.DataFrame(columns=columns)
    for (pair, records) in transitions.groupby(by=['source_activity', 'source_resource',
                                                   'destination_activity', 'destination_resource']):
        transition_with_frequency = pd.concat([transition_with_frequency, pd.DataFrame({
            'source_activity': [pair[0]],
            'source_resource': [pair[1]],
            'destination_activity': [pair[2]],
            'destination_resource': [pair[3]],
            'frequency': [len(records)],
            'transition_type': [records['transition_type'].iloc[0]],
            log_ids.wt_total: [records[log_ids.wt_total].sum()],
            log_ids.wt_batching: [records[log_ids.wt_batching].sum()],
            log_ids.wt_prioritization: [records[log_ids.wt_prioritization].sum()],
            log_ids.wt_contention: [records[log_ids.wt_contention].sum()],
            log_ids.wt_unavailability: [records[log_ids.wt_unavailability].sum()],
            log_ids.wt_extraneous: [records[log_ids.wt_extraneous].sum()],
        })], ignore_index=True)
    return transition_with_frequency


def __create_single_dataframe(items: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
    items = list(filter(lambda df: not df.empty, items))
    if len(items) == 0:
        return None
    else:
        return pd.concat(items).reset_index(drop=True)


def __join_per_case_items(items: List[pd.DataFrame], log_ids: Optional[EventLogIDs] = None) -> Optional[pd.DataFrame]:
    """Joins a list of items summing up frequency and duration."""

    log_ids = log_ids_non_nil(log_ids)

    items = list(filter(lambda df: not df.empty, items))

    if len(items) == 0:
        return None

    columns = ['source_activity', 'source_resource', 'destination_activity', 'destination_resource']
    grouped = pd.concat(items).groupby(columns)
    result = pd.DataFrame(columns=columns)
    for pair_index, group in grouped:
        source_activity, source_resource, destination_activity, destination_resource = pair_index
        group_wt_total: pd.Timedelta = group[log_ids.wt_total].sum()

        group_wt_batching = pd.Timedelta(0)
        if log_ids.wt_batching in group.columns:
            group_wt_batching = group[log_ids.wt_batching].sum()

        group_wt_prioritization = pd.Timedelta(0)
        if log_ids.wt_prioritization in group.columns:
            group_wt_prioritization = group[log_ids.wt_prioritization].sum()

        group_wt_contention = pd.Timedelta(0)
        if log_ids.wt_contention in group.columns:
            group_wt_contention = pd.to_timedelta(group[log_ids.wt_contention]).sum()

        group_wt_unavailability = pd.Timedelta(0)
        if log_ids.wt_unavailability in group.columns:
            group_wt_unavailability = pd.to_timedelta(group[log_ids.wt_unavailability]).sum()

        group_wt_extraneous = pd.Timedelta(0)
        if log_ids.wt_extraneous in group.columns:
            group_wt_extraneous = pd.to_timedelta(group[log_ids.wt_extraneous]).sum()

        group_frequency: float = group['frequency'].sum()
        group_case_id: str = ','.join(group['case_id'].astype(str).unique())
        result = pd.concat([result, pd.DataFrame({
            'source_activity': [source_activity],
            'source_resource': [source_resource],
            'destination_activity': [destination_activity],
            'destination_resource': [destination_resource],
            'frequency': [group_frequency],
            'cases': [group_case_id],
            log_ids.wt_total: [group_wt_total],
            log_ids.wt_batching: [group_wt_batching],
            log_ids.wt_prioritization: [group_wt_prioritization],
            log_ids.wt_contention: [group_wt_contention],
            log_ids.wt_unavailability: [group_wt_unavailability],
            log_ids.wt_extraneous: [group_wt_extraneous]
        })], ignore_index=True)
    result.reset_index(drop=True, inplace=True)
    return result

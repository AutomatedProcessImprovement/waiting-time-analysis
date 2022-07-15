import concurrent.futures
import multiprocessing
from typing import Dict, Optional

import click
import numpy as np
import pandas as pd
from tqdm import tqdm

import process_waste.helpers
from batch_processing_analysis.config import EventLogIDs
from process_waste import GRANULARITY_MINUTES
from process_waste.helpers import WAITING_TIME_TOTAL_KEY, WAITING_TIME_BATCHING_KEY, WAITING_TIME_CONTENTION_KEY, \
    WAITING_TIME_PRIORITIZATION_KEY, WAITING_TIME_UNAVAILABILITY_KEY, WAITING_TIME_EXTRANEOUS_KEY, \
    print_section_boundaries, convert_timestamp_columns_to_datetime, log_ids_non_nil
from process_waste.waiting_time import analysis as wt_analysis


@print_section_boundaries('Activity Transitions Analysis')
def identify(
        log: pd.DataFrame,
        parallel_activities: Dict[str, set],
        parallel_run=True,
        log_ids: Optional[EventLogIDs] = None,
        calendar: Optional[Dict] = None) -> Optional[pd.DataFrame]:
    from process_waste.calendar.calendar import make as make_calendar

    click.echo(f'Parallel run: {parallel_run}')

    log_ids = log_ids_non_nil(log_ids)

    if not calendar:
        log_calendar = make_calendar(log, granularity=GRANULARITY_MINUTES, log_ids=log_ids)
    else:
        log_calendar = calendar

    log_grouped = log.groupby(by=log_ids.case)
    all_items = []
    if parallel_run:
        n_cores = multiprocessing.cpu_count() - 1
        handles = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=n_cores) as executor:
            for (case_id, case) in tqdm(log_grouped, desc='Submitting tasks for concurrent execution'):
                case = case.sort_values(by=[log_ids.end_time, log_ids.start_time])
                handle = executor.submit(__identify_transitions_per_case_and_make_report,
                                         case,
                                         parallel_activities=parallel_activities,
                                         case_id=case_id,
                                         log_calendar=log_calendar,
                                         log=log,
                                         log_ids=log_ids)
                handles.append(handle)

        for h in tqdm(handles, desc='Waiting for tasks to finish'):
            done = h.done()
            result = h.result()
            if done and not result.empty:
                all_items.append(result)
    else:
        for (case_id, case) in tqdm(log_grouped, desc='Processing cases'):
            case = case.sort_values(by=[log_ids.end_time, log_ids.start_time])
            result = __identify_transitions_per_case_and_make_report(case,
                                                                     parallel_activities=parallel_activities,
                                                                     case_id=case_id,
                                                                     log_calendar=log_calendar,
                                                                     log=log,
                                                                     log_ids=log_ids)
            if result is not None:
                all_items.append(result)

    if len(all_items) == 0:
        return None

    result: pd.DataFrame = process_waste.helpers.join_per_case_items(all_items)

    if result is not None:
        result['wt_total_seconds'] = result[WAITING_TIME_TOTAL_KEY] / np.timedelta64(1, 's')

    return result


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

    transitions_with_frequency = __calculate_frequency_and_duration(transitions)

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

    case[wt_analysis.TRANSITION_COLUMN_KEY] = np.NAN

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
                case.at[index, wt_analysis.TRANSITION_COLUMN_KEY] = previous_event_index
                non_concurrent_previous_event_found = True


def __calculate_frequency_and_duration(transitions: pd.DataFrame) -> pd.DataFrame:
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
            'handoff_type': [records['handoff_type'].iloc[0]],
            WAITING_TIME_TOTAL_KEY: [records[WAITING_TIME_TOTAL_KEY].sum()],
            WAITING_TIME_BATCHING_KEY: [records[WAITING_TIME_BATCHING_KEY].sum()],
            WAITING_TIME_PRIORITIZATION_KEY: [records[WAITING_TIME_PRIORITIZATION_KEY].sum()],
            WAITING_TIME_CONTENTION_KEY: [records[WAITING_TIME_CONTENTION_KEY].sum()],
            WAITING_TIME_UNAVAILABILITY_KEY: [records[WAITING_TIME_UNAVAILABILITY_KEY].sum()],
            WAITING_TIME_EXTRANEOUS_KEY: [records[WAITING_TIME_EXTRANEOUS_KEY].sum()],
        })], ignore_index=True)
    return transition_with_frequency

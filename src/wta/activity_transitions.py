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

total = []


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

    return all_items


def __sequential_run(log, log_ids, calendar, parallel_activities):
    results_transitions = []
    log_grouped = log.groupby(by=log_ids.case)

    for (case_id, case) in log_grouped:
        case = case.sort_values(by=[log_ids.end_time, log_ids.start_time])
        transitions = __identify_transitions_per_case_and_make_report(
            case,
            parallel_activities=parallel_activities,
            case_id=case_id,
            log_calendar=calendar,
            log=log,
            log_ids=log_ids)

        if transitions is not None:
            results_transitions.append(transitions)

    if len(results_transitions) > 0:
        concatenated_transitions = pd.concat(results_transitions, ignore_index=True)
        concatenated_transitions = convert_time_columns_to_seconds(concatenated_transitions)
        return concatenated_transitions
    else:
        return None


def __multiprocess_run(log, log_ids, calendar, parallel_activities):
    all_transitions = []
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
        if done:
            result_transitions = h.result()
            if not result_transitions.empty:
                all_transitions.append(result_transitions)

    # Concatenate the DataFrames vertically into a single DataFrame
    concatenated_transitions = pd.concat(all_transitions, ignore_index=True)
    concatenated_transitions = convert_time_columns_to_seconds(concatenated_transitions)
    try:
        ordered_columns = [
            'start_time',
            'end_time',
            'source_activity',
            'source_resource',
            'destination_activity',
            'destination_resource',
            'case_id',
            'transition_type',
            'wt_total',
            'wt_contention',
            'wt_batching',
            'wt_prioritization',
            'wt_unavailability',
            'wt_extraneous'
        ]

        concatenated_transitions = concatenated_transitions[ordered_columns]

    except KeyError as e:
        print(f"Error: {e}.")

    return concatenated_transitions


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

    # attaching case ID as additional information
    transitions['case_id'] = case_id

    return transitions


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


def time_to_seconds(t):
    if pd.isnull(t):
        return None
    elif isinstance(t, pd.Timedelta):
        return t.total_seconds()
    else:
        try:
            # Try to interpret the string as a time format "HH:MM:SS"
            time_parts = str(t).split(' ')[-1].split(':')
            return int(time_parts[0]) * 3600 + int(time_parts[1]) * 60 + int(time_parts[2])
        except ValueError:
            # If the string is not in the expected format, try to interpret it as seconds
            try:
                return float(t)
            except ValueError:
                # If the string can't be interpreted as a float, return None
                return None


convert_columns = [
    'wt_total',
    'wt_contention',
    'wt_batching',
    'wt_prioritization',
    'wt_unavailability',
    'wt_extraneous'
]


def convert_time_columns_to_seconds(df: pd.DataFrame):
    for col in convert_columns:
        df[col] = df[col].map(time_to_seconds)
    return df

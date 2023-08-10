import concurrent.futures
import multiprocessing
from typing import Dict, Optional, Union

import click
import numpy as np
import pandas as pd
from tqdm import tqdm

from wta import GRANULARITY_MINUTES
from wta.helpers import print_section_boundaries, convert_timestamp_columns_to_datetime, log_ids_non_nil, \
    EventLogIDs, TRANSITION_COLUMN_KEY
from wta.waiting_time import analysis as wt_analysis
from wta.calendars.calendars import make as make_calendar


CONVERT_COLUMNS = ['wt_total', 'wt_contention', 'wt_batching', 'wt_prioritization', 'wt_unavailability', 'wt_extraneous']
ORDERED_COLUMNS = [
    'start_time',
    'end_time',
    'source_activity',
    'source_resource',
    'destination_activity',
    'destination_resource',
    'case_id',
    'wt_total',
    'wt_contention',
    'wt_batching',
    'wt_prioritization',
    'wt_unavailability',
    'wt_extraneous'
]


@print_section_boundaries('Activity Transitions Analysis')
def identify(log: pd.DataFrame, parallel_activities: Dict[str, set], parallel_run: bool = True,
             log_ids: Optional[EventLogIDs] = None, calendar: Optional[Dict] = None) -> Optional[pd.DataFrame]:
    click.echo(f'Parallel run: {parallel_run}')
    log_ids = log_ids_non_nil(log_ids)
    log_calendar = make_calendar_if_none(log, log_ids, calendar)
    run_func = __multiprocess_run if parallel_run else __sequential_run
    all_items = run_func(log, log_ids, log_calendar, parallel_activities)
    return None if len(all_items) == 0 else all_items


def make_calendar_if_none(log, log_ids, calendar):
    return make_calendar(log, granularity=GRANULARITY_MINUTES, log_ids=log_ids) if not calendar else calendar


def __sequential_run(log, log_ids, calendar, parallel_activities):
    log_grouped = log.groupby(by=log_ids.case)
    results_transitions = [identify_transitions_and_report(sort_case(case, log_ids), parallel_activities, case_id, calendar, log, log_ids)
                           for case_id, case in log_grouped]
    return concatenate_transitions_if_exists(results_transitions)


def __multiprocess_run(log, log_ids, calendar, parallel_activities):
    n_cores = multiprocessing.cpu_count() - 1
    handles = []
    log_grouped = log.groupby(by=log_ids.case)

    with concurrent.futures.ProcessPoolExecutor(max_workers=n_cores) as executor:
        handles = [submit_task(executor, sort_case(case, log_ids), parallel_activities, case_id, calendar, log, log_ids)
                   for case_id, case in tqdm(log_grouped, desc='Submitting tasks for concurrent execution')]

    all_transitions = [h.result() for h in tqdm(handles, desc='Waiting for tasks to finish') if not h.result().empty]
    return concatenate_transitions_if_exists(all_transitions)


def sort_case(case, log_ids):
    return case.sort_values(by=[log_ids.end_time, log_ids.start_time])


def submit_task(executor, case, parallel_activities, case_id, calendar, log, log_ids):
    return executor.submit(identify_transitions_and_report, case, parallel_activities, case_id, calendar, log, log_ids)


def concatenate_transitions_if_exists(results_transitions):
    return convert_time_columns_to_seconds(pd.concat(results_transitions, ignore_index=True)) if results_transitions else None


def identify_transitions_and_report(case, parallel_activities, case_id, log_calendar, log, log_ids):
    case = convert_timestamp_columns_to_datetime(case, log_ids)
    log = convert_timestamp_columns_to_datetime(log, log_ids)
    mark_activity_transitions(case, parallel_activities, log_ids=log_ids)
    transitions = wt_analysis.run(case, log_calendar, log, log_ids=log_ids)
    transitions['case_id'] = case_id
    return transitions


def mark_activity_transitions(case, parallel_activities, log_ids):
    case[log_ids.transition_source_index] = np.NAN
    reversed_index = list(reversed(case.index))
    for i in range(len(reversed_index)):
        non_concurrent_previous_event_found = False
        index = reversed_index[i]
        current_event = case.loc[index]
        parallel_activities_for_current_event = parallel_activities.get(current_event[log_ids.activity], [])
        previous_event_index_delta = i + 1
        while not non_concurrent_previous_event_found and previous_event_index_delta <= len(reversed_index) - 1:
            previous_event_index = reversed_index[previous_event_index_delta]
            previous_event = case.loc[previous_event_index]
            overlapping_activity_instances = previous_event[log_ids.end_time] > current_event[log_ids.start_time]
            if previous_event[log_ids.activity] in parallel_activities_for_current_event or overlapping_activity_instances:
                previous_event_index_delta += 1
            else:
                case.at[index, TRANSITION_COLUMN_KEY] = previous_event_index
                non_concurrent_previous_event_found = True


def convert_time_columns_to_seconds(df: pd.DataFrame):
    df[CONVERT_COLUMNS] = df[CONVERT_COLUMNS].applymap(time_to_seconds)
    return df[ORDERED_COLUMNS] if ORDERED_COLUMNS else df


def time_to_seconds(t: Union[pd.Timedelta, float, str, None]) -> Union[float, None]:
    if pd.isnull(t):
        return None
    elif isinstance(t, pd.Timedelta):
        return t.total_seconds()
    else:
        try:
            time_parts = str(t).split(' ')[-1].split(':')
            return int(time_parts[0]) * 3600 + int(time_parts[1]) * 60 + int(time_parts[2])
        except ValueError:
            try:
                return float(t)
            except ValueError:
                return None

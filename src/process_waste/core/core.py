import concurrent.futures
import multiprocessing
from pathlib import Path
from typing import List, Optional, Dict, Tuple

import click
import numpy as np
import pandas as pd
from estimate_start_times.concurrency_oracle import HeuristicsConcurrencyOracle
from estimate_start_times.config import Configuration, ReEstimationMethod, ConcurrencyOracleType, \
    ResourceAvailabilityType, \
    HeuristicsThresholds, EventLogIDs
from tqdm import tqdm

END_TIMESTAMP_KEY = 'time:timestamp'
ACTIVITY_KEY = 'concept:name'
CASE_KEY = 'case:concept:name'
RESOURCE_KEY = 'org:resource'
START_TIMESTAMP_KEY = 'start_timestamp'
ENABLED_TIMESTAMP_KEY = 'enabled_timestamp'
AVAILABLE_TIMESTAMP_KEY = 'available_timestamp'
TRANSITION_KEY = 'lifecycle:transition'
WAITING_TIME_TOTAL_KEY = 'wt_total'
WAITING_TIME_BATCHING_KEY = 'wt_batching'
WAITING_TIME_CONTENTION_KEY = 'wt_contention'
WAITING_TIME_PRIORITIZATION_KEY = 'wt_prioritization'
WAITING_TIME_UNAVAILABILITY_KEY = 'wt_unavailability'
WAITING_TIME_EXTRANEOUS_KEY = 'wt_extraneous'

GRANULARITY_MINUTES = 15

default_log_ids = EventLogIDs(  # TODO: extend EventLogIDs with waiting time columns
    case=CASE_KEY,
    activity=ACTIVITY_KEY,
    start_time=START_TIMESTAMP_KEY,
    end_time=END_TIMESTAMP_KEY,
    enabled_time=ENABLED_TIMESTAMP_KEY,
    available_time=AVAILABLE_TIMESTAMP_KEY,
    resource=RESOURCE_KEY,
    lifecycle=TRANSITION_KEY,
)

default_configuration = Configuration(
    log_ids=default_log_ids,
    concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
    resource_availability_type=ResourceAvailabilityType.SIMPLE,
    heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9)
)


def parallel_activities_with_heuristic_oracle(log: pd.DataFrame, log_ids: Optional[EventLogIDs] = None) -> Dict[
    str, set]:
    if not log_ids:
        log_ids = default_log_ids

    config = Configuration(
        log_ids=log_ids,
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        bot_resources={"Start", "End"},
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9)
    )
    oracle = HeuristicsConcurrencyOracle(log, config)
    return oracle.concurrency


def parallel_activities_with_alpha_oracle(df: pd.DataFrame) -> List[tuple]:
    df = df.sort_values(by=[START_TIMESTAMP_KEY, END_TIMESTAMP_KEY, CASE_KEY])
    activities_names = df[ACTIVITY_KEY].unique()
    matrix = pd.DataFrame(0, index=activities_names, columns=activities_names)

    # per group
    df_grouped = df.groupby(by=CASE_KEY)
    for case_id, case in df_grouped:
        case_shifted = case.shift(-1)
        # dropping N/A
        activities = case.drop(index=case.tail(1).index)
        case_shifted = case_shifted.drop(index=case_shifted.tail(1).index)
        if case_shifted.size != activities.size:
            raise Exception("Arrays' sizes must be equal")

        for i in range(len(activities)):
            if activities.iloc[i][END_TIMESTAMP_KEY] < case_shifted.iloc[i][END_TIMESTAMP_KEY]:
                (row, column) = (activities[ACTIVITY_KEY].iloc[i], case_shifted[ACTIVITY_KEY].iloc[i])
                matrix.at[row, column] += 1

    parallel_activities = set()
    for row in activities_names:
        parallel_pair = set()
        for column in activities_names:
            if (matrix.at[row, column] > 0) and (matrix.at[column, row] > 0):
                parallel_pair.add(row)
                parallel_pair.add(column)
        if len(parallel_pair) > 0:
            parallel_activities.add(tuple(parallel_pair))

    return list(parallel_activities)


def timezone_aware_subtraction(df1: pd.DataFrame, df2: pd.DataFrame,
                               df1_col_name: str, df2_col_name: Optional[str] = None) -> pd.DataFrame:
    if df2_col_name is None:
        df2_col_name = df1_col_name
    return df1[df1_col_name].dt.tz_convert(tz='UTC') - df2[df2_col_name].dt.tz_convert(tz='UTC')


def identify_main(
        log: pd.DataFrame,
        parallel_activities: Dict[str, set],
        identify_fn_per_case,
        join_fn,
        parallel_run=True,
        log_ids: Optional[EventLogIDs] = None) -> Optional[pd.DataFrame]:
    from process_waste.calendar import calendar

    if not log_ids:
        log_ids = default_log_ids

    log_grouped = log.groupby(by=log_ids.case)
    all_items = []
    log_calendar = calendar.make(log, granularity=GRANULARITY_MINUTES, log_ids=log_ids)
    if parallel_run:
        n_cores = multiprocessing.cpu_count() - 1
        handles = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=n_cores) as executor:
            for (case_id, case) in tqdm(log_grouped, desc='Submitting tasks for concurrent execution'):
                case = case.sort_values(by=[log_ids.end_time, log_ids.start_time])
                handle = executor.submit(identify_fn_per_case,
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
            result = identify_fn_per_case(case,
                                          parallel_activities=parallel_activities,
                                          case_id=case_id,
                                          log_calendar=log_calendar,
                                          log=log,
                                          log_ids=log_ids)
            if result is not None:
                all_items.append(result)

    if len(all_items) == 0:
        return None

    result: pd.DataFrame = join_fn(all_items)
    result['wt_total_seconds'] = result[WAITING_TIME_TOTAL_KEY] / np.timedelta64(1, 's')
    return result


def join_per_case_items(items: List[pd.DataFrame]) -> pd.DataFrame:
    """Joins a list of items summing up frequency and duration."""
    columns = ['source_activity', 'source_resource', 'destination_activity', 'destination_resource']
    grouped = pd.concat(items).groupby(columns)
    result = pd.DataFrame(columns=columns)
    for pair_index, group in grouped:
        source_activity, source_resource, destination_activity, destination_resource = pair_index
        group_wt_total: pd.Timedelta = group[WAITING_TIME_TOTAL_KEY].sum()

        group_wt_batching = 0
        if WAITING_TIME_BATCHING_KEY in group.columns:
            group_wt_batching = group[WAITING_TIME_BATCHING_KEY].sum()

        group_wt_prioritization = 0
        if WAITING_TIME_PRIORITIZATION_KEY in group.columns:
            group_wt_prioritization = group[WAITING_TIME_PRIORITIZATION_KEY].sum()

        group_wt_contention = 0
        if WAITING_TIME_CONTENTION_KEY in group.columns:
            group_wt_contention = pd.to_timedelta(group[WAITING_TIME_CONTENTION_KEY]).sum()

        group_wt_unavailability = 0
        if WAITING_TIME_UNAVAILABILITY_KEY in group.columns:
            group_wt_unavailability = pd.to_timedelta(group[WAITING_TIME_UNAVAILABILITY_KEY]).sum()

        group_wt_extraneous = 0
        if WAITING_TIME_EXTRANEOUS_KEY in group.columns:
            group_wt_extraneous = pd.to_timedelta(group[WAITING_TIME_EXTRANEOUS_KEY]).sum()

        group_frequency: float = group['frequency'].sum()
        group_case_id: str = ','.join(group['case_id'].astype(str).unique())
        result = pd.concat([result, pd.DataFrame({
            'source_activity': [source_activity],
            'source_resource': [source_resource],
            'destination_activity': [destination_activity],
            'destination_resource': [destination_resource],
            'frequency': [group_frequency],
            'cases': [group_case_id],
            WAITING_TIME_TOTAL_KEY: [group_wt_total],
            WAITING_TIME_BATCHING_KEY: [group_wt_batching],
            WAITING_TIME_PRIORITIZATION_KEY: [group_wt_prioritization],
            WAITING_TIME_CONTENTION_KEY: [group_wt_contention],
            WAITING_TIME_UNAVAILABILITY_KEY: [group_wt_unavailability],
            WAITING_TIME_EXTRANEOUS_KEY: [group_wt_extraneous]
        })], ignore_index=True)
    result.reset_index(drop=True, inplace=True)
    return result


def add_enabled_timestamp(log: pd.DataFrame):
    global default_configuration
    oracle = HeuristicsConcurrencyOracle(log, default_configuration)
    oracle.add_enabled_times(log)


def read_csv(log_path: Path, log_ids: Optional[EventLogIDs] = None, utc: bool = True) -> pd.DataFrame:
    log = pd.read_csv(log_path)

    if not log_ids:
        log_ids = default_log_ids

    log = convert_timestamp_columns_to_datetime(log, log_ids, utc=utc)

    duration_columns = [WAITING_TIME_TOTAL_KEY, WAITING_TIME_BATCHING_KEY]
    for column in duration_columns:
        if column in log.columns:
            log[column] = pd.to_timedelta(log[column])

    return log


def print_section_boundaries(title: Optional[str] = None):
    """Decorator that pretty-prints the result of the analysis"""

    def decorator(func):
        def wrapper(*args, **kwargs):
            click.echo('\n' + '-' * 80)
            if title:
                click.echo(title)
            else:
                click.echo(func.__name__)
            click.echo('-' * 80)
            result = func(*args, **kwargs)
            click.echo('-' * 80)
            return result

        return wrapper

    return decorator


def convert_timestamp_columns_to_datetime(
        event_log: pd.DataFrame,
        log_ids: EventLogIDs,
        time_columns: Tuple[str] = None,
        utc: bool = True) -> pd.DataFrame:
    """Converts the timestamp columns of the event log to datetime."""

    if not time_columns:
        time_columns = [log_ids.start_time, log_ids.end_time, log_ids.enabled_time]
    for column in time_columns:
        if column in event_log.columns:
            event_log[column] = pd.to_datetime(event_log[column], utc=utc)

    return event_log

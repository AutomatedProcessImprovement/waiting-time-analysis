import concurrent.futures
import multiprocessing
from pathlib import Path
from typing import List, Optional, Dict

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

default_configuration = Configuration(
    log_ids=EventLogIDs(
        case=CASE_KEY,
        activity=ACTIVITY_KEY,
        start_time=START_TIMESTAMP_KEY,
        end_time=END_TIMESTAMP_KEY,
        enabled_time=ENABLED_TIMESTAMP_KEY,
        available_time=AVAILABLE_TIMESTAMP_KEY,
        resource=RESOURCE_KEY,
        lifecycle=TRANSITION_KEY,
    ),
    concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
    resource_availability_type=ResourceAvailabilityType.SIMPLE,
    heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9)
)


def parallel_activities_with_heuristic_oracle(log: pd.DataFrame) -> Dict[str, set]:
    column_names = EventLogIDs(
        case=CASE_KEY,
        activity=ACTIVITY_KEY,
        start_time=START_TIMESTAMP_KEY,
        end_time=END_TIMESTAMP_KEY,
        resource=RESOURCE_KEY,
        lifecycle=TRANSITION_KEY
    )
    config = Configuration(
        log_ids=column_names,
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


def identify_main(log: pd.DataFrame, parallel_activities: Dict[str, set], identify_fn_per_case, join_fn,
                  parallel_run=True) -> Optional[pd.DataFrame]:
    log_grouped = log.groupby(by=CASE_KEY)
    all_items = []
    if parallel_run:
        n_cores = multiprocessing.cpu_count() - 1
        handles = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=n_cores) as executor:
            for (case_id, case) in tqdm(log_grouped, desc='submitting tasks for concurrent execution'):
                case = case.sort_values(by=[END_TIMESTAMP_KEY, START_TIMESTAMP_KEY])
                handle = executor.submit(identify_fn_per_case, case, parallel_activities, case_id)
                handles.append(handle)

        for h in tqdm(handles, desc='waiting for tasks to finish'):
            done = h.done()
            result = h.result()
            if done and not result.empty:
                all_items.append(result)
    else:
        for (case_id, case) in tqdm(log_grouped, desc='processing cases'):
            case = case.sort_values(by=[END_TIMESTAMP_KEY, START_TIMESTAMP_KEY])
            result = identify_fn_per_case(case, parallel_activities, case_id)
            if result is not None:
                all_items.append(result)

    if len(all_items) == 0:
        return None

    result: pd.DataFrame = join_fn(all_items)
    result['duration_sum_seconds'] = result['duration_sum'] / np.timedelta64(1, 's')
    return result


def join_per_case_items(items: List[pd.DataFrame]) -> pd.DataFrame:
    """Joins a list of items summing up frequency and duration."""
    columns = ['source_activity', 'source_resource', 'destination_activity', 'destination_resource']
    grouped = pd.concat(items).groupby(columns)
    result = pd.DataFrame(columns=columns)
    for pair_index, group in grouped:
        source_activity, source_resource, destination_activity, destination_resource = pair_index
        group_duration: pd.Timedelta = group['duration'].sum()
        group_frequency: float = group['frequency'].sum()
        group_case_id: str = ','.join(group['case_id'].astype(str).unique())
        result = pd.concat([result, pd.DataFrame({
            'source_activity': [source_activity],
            'source_resource': [source_resource],
            'destination_activity': [destination_activity],
            'destination_resource': [destination_resource],
            'duration_sum': [group_duration],
            'frequency': [group_frequency],
            'cases': [group_case_id]
        })], ignore_index=True)
    result.reset_index(drop=True, inplace=True)
    return result


def add_enabled_timestamp(log: pd.DataFrame):
    global default_configuration
    oracle = HeuristicsConcurrencyOracle(log, default_configuration)
    oracle.add_enabled_times(log)


def read_csv(log_path: Path) -> pd.DataFrame:
    log = pd.read_csv(log_path)

    time_columns = ['start_timestamp', 'time:timestamp']
    for column in time_columns:
        log[column] = pd.to_datetime(log[column])

    return log

import concurrent.futures
import multiprocessing
from pathlib import Path
from typing import Tuple, List, Optional, Dict

import numpy as np
import pandas as pd
from config import Configuration, ReEstimationMethod, ConcurrencyOracleType, ResourceAvailabilityType, \
    HeuristicsThresholds, EventLogIDs
from data_frame.concurrency_oracle import HeuristicsConcurrencyOracle
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.log.util import interval_lifecycle
from pm4py.statistics.concurrent_activities.pandas import get as concurrent_activities_get


def lifecycle_to_interval(log_path: Path) -> pd.DataFrame:
    log = xes_importer.apply(str(log_path))
    log_interval = interval_lifecycle.to_interval(log)
    event_log_interval = log_converter.apply(log_interval, variant=log_converter.Variants.TO_DATA_FRAME)
    return event_log_interval


def get_concurrent_activities(case: pd.DataFrame) -> list[Tuple]:
    case = case.sort_values(by=['start_timestamp', 'time:timestamp'])

    def _preprocess_case(case: pd.DataFrame):
        # subtracting a microsecond from `time:timestamp` to avoid touching events to be concurrent ones
        case['time:timestamp'] = case['time:timestamp'] - pd.Timedelta('1 us')
        return case

    def _postprocess_case(case: pd.DataFrame):
        # subtracting a microsecond from `time:timestamp` to avoid touching events to be concurrent ones
        case['time:timestamp'] = case['time:timestamp'] + pd.Timedelta('1 us')
        return case

    case = _preprocess_case(case)

    params = {concurrent_activities_get.Parameters.TIMESTAMP_KEY: "time:timestamp",
              concurrent_activities_get.Parameters.START_TIMESTAMP_KEY: "start_timestamp"}
    concurrent_activities = concurrent_activities_get.apply(case, parameters=params)
    result = [activities for activities in concurrent_activities]

    _postprocess_case(case)
    return result


def add_enabled_timestamps(event_log: pd.DataFrame,
                           concurrent_activities: Optional[List[tuple]] = None) -> pd.DataFrame:
    enabled_timestamp_key = 'enabled_timestamp'
    start_timestamp_key = 'start_timestamp'
    end_timestamp_key = 'time:timestamp'

    event_log = event_log.sort_values(by='time:timestamp')

    # default enabled timestamps are start timestamps
    event_log[enabled_timestamp_key] = event_log[start_timestamp_key]

    if concurrent_activities is None:
        concurrent_activities = get_concurrent_activities(event_log)  # NOTE: per case concurrency identification

    for i in event_log.index:
        activity_name = event_log.loc[i]['concept:name']
        start = event_log.loc[i][start_timestamp_key]

        concurrent_activities_names = None
        for item in concurrent_activities:
            if activity_name in item:
                concurrent_activities_names = item
                break

        query = '`time:timestamp` <= @start & `concept:name` != @activity_name'
        if concurrent_activities_names:
            query += ' & `concept:name` not in @concurrent_activities_names'

        ended_before = event_log.query(query)
        if ended_before is not None and not ended_before.empty:
            enabled_timestamp = ended_before[end_timestamp_key].max()
            event_log.at[i, enabled_timestamp_key] = enabled_timestamp

    event_log[enabled_timestamp_key] = pd.to_datetime(event_log[enabled_timestamp_key])
    event_log[end_timestamp_key] = pd.to_datetime(event_log[end_timestamp_key])

    return event_log


def parallel_activities_with_heuristic_oracle(log: pd.DataFrame) -> Dict[str, set]:
    column_names = EventLogIDs(
        case='case:concept:name',
        activity='concept:name',
        start_timestamp='start_timestamp',
        end_timestamp='time:timestamp',
        resource='org:resource',
        lifecycle='lifecycle:transition'
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
    df = df.sort_values(by=['start_timestamp', 'time:timestamp', 'case:concept:name'])
    activities_names = df['concept:name'].unique()
    matrix = pd.DataFrame(0, index=activities_names, columns=activities_names)

    # per group
    df_grouped = df.groupby(by='case:concept:name')
    for case_id, case in df_grouped:
        case_shifted = case.shift(-1)
        # dropping N/A
        activities = case.drop(index=case.tail(1).index)
        case_shifted = case_shifted.drop(index=case_shifted.tail(1).index)
        if case_shifted.size != activities.size:
            raise Exception("Arrays' sizes must be equal")

        for i in range(len(activities)):
            if activities.iloc[i]['time:timestamp'] < case_shifted.iloc[i]['time:timestamp']:
                (row, column) = (activities['concept:name'].iloc[i], case_shifted['concept:name'].iloc[i])
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


def concurrent_activities_by_time(df: pd.DataFrame) -> List[tuple]:
    parallel_activities = set()

    # per group
    df_grouped = df.groupby(by='case:concept:name')
    for case_id, case in df_grouped:
        activities = get_concurrent_activities(case)
        if len(activities) == 0:
            continue
        for concurrent in activities:
            parallel_activities.add(concurrent)

    return list(parallel_activities)


def timezone_aware_subtraction(df1: pd.DataFrame, df2: pd.DataFrame,
                               df1_col_name: str, df2_col_name: Optional[str] = None) -> pd.DataFrame:
    if df2_col_name is None:
        df2_col_name = df1_col_name
    return df1[df1_col_name].dt.tz_convert(tz='UTC') - df2[df2_col_name].dt.tz_convert(tz='UTC')


def identify_main(log_path: Path, identify_fn_per_case, join_fn, parallel_run=True) -> Optional[pd.DataFrame]:
    log = lifecycle_to_interval(log_path)
    parallel_activities = parallel_activities_with_heuristic_oracle(log)

    log_grouped = log.groupby(by='case:concept:name')
    all_items = []
    if parallel_run:
        n_cores = multiprocessing.cpu_count() - 1
        handles = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=n_cores) as executor:
            for (case_id, case) in log_grouped:
                case = case.sort_values(by=['time:timestamp', 'start_timestamp'])
                handle = executor.submit(identify_fn_per_case, case, parallel_activities, case_id)
                handles.append(handle)

        for h in handles:
            done = h.done()
            result = h.result()
            if done and not result.empty:
                all_items.append(result)
    else:
        for (case_id, case) in log_grouped:
            case = case.sort_values(by=['time:timestamp', 'start_timestamp'])
            result = identify_fn_per_case(case, parallel_activities, case_id)
            if result is not None:
                all_items.append(result)

    if len(all_items) == 0:
        return None

    result: pd.DataFrame = join_fn(all_items)
    result['duration_sum_seconds'] = result['duration_sum'] / np.timedelta64(1, 's')
    return result


def join_per_case_items(items: list[pd.DataFrame]) -> pd.DataFrame:
    """Joins a list of items summing up frequency and duration."""
    columns = ['source_activity', 'source_resource', 'destination_activity', 'destination_resource']
    grouped = pd.concat(items).groupby(columns)
    result = pd.DataFrame(columns=columns)
    for pair_index, group in grouped:
        source_activity, source_resource, destination_activity, destination_resource = pair_index
        group_duration: pd.Timedelta = group['duration'].sum()
        group_frequency: float = group['frequency'].sum()
        group_case_id: str = ','.join(group['case_id'].astype(str).unique())
        result = result.append({
            'source_activity': source_activity,
            'source_resource': source_resource,
            'destination_activity': destination_activity,
            'destination_resource': destination_resource,
            'duration_sum': group_duration,
            'frequency': group_frequency,
            'cases': group_case_id
        }, ignore_index=True)
    result.reset_index(drop=True, inplace=True)
    return result

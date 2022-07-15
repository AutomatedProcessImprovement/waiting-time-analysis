from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import click
import pandas as pd

from batch_processing_analysis.config import EventLogIDs as batch_processing_analysis_EventLogIDs
from estimate_start_times.concurrency_oracle import HeuristicsConcurrencyOracle
from estimate_start_times.config import Configuration, ConcurrencyOracleType, ResourceAvailabilityType, \
    HeuristicsThresholds, ReEstimationMethod

END_TIMESTAMP_KEY = 'time:timestamp'
ACTIVITY_KEY = 'concept:name'
CASE_KEY = 'case:concept:name'
RESOURCE_KEY = 'org:resource'
START_TIMESTAMP_KEY = 'start_timestamp'
ENABLED_TIMESTAMP_KEY = 'enabled_timestamp'
TRANSITION_COLUMN_KEY = 'transition_source_index'

WAITING_TIME_TOTAL_KEY = 'wt_total'
WAITING_TIME_BATCHING_KEY = 'wt_batching'
WAITING_TIME_CONTENTION_KEY = 'wt_contention'
WAITING_TIME_PRIORITIZATION_KEY = 'wt_prioritization'
WAITING_TIME_UNAVAILABILITY_KEY = 'wt_unavailability'
WAITING_TIME_EXTRANEOUS_KEY = 'wt_extraneous'

BATCH_INSTANCE_ENABLED_KEY = 'batch_instance_enabled'
BATCH_INSTANCE_ID_KEY = 'batch_instance_id'

CTE_IMPACT_KEY = 'cte_impact'

GRANULARITY_MINUTES = 15


@dataclass
class EventLogIDs(batch_processing_analysis_EventLogIDs):
    """Extended log IDs with waiting time and CTE impact keys."""

    wt_total: str = WAITING_TIME_TOTAL_KEY
    wt_batching: str = WAITING_TIME_BATCHING_KEY
    wt_contention: str = WAITING_TIME_CONTENTION_KEY
    wt_prioritization: str = WAITING_TIME_PRIORITIZATION_KEY
    wt_unavailability: str = WAITING_TIME_UNAVAILABILITY_KEY
    wt_extraneous: str = WAITING_TIME_EXTRANEOUS_KEY
    cte_impact: str = CTE_IMPACT_KEY
    transition_source_index: str = TRANSITION_COLUMN_KEY


default_log_ids = EventLogIDs(
    case=CASE_KEY,
    activity=ACTIVITY_KEY,
    start_time=START_TIMESTAMP_KEY,
    end_time=END_TIMESTAMP_KEY,
    enabled_time=ENABLED_TIMESTAMP_KEY,
    resource=RESOURCE_KEY,
)

default_configuration = Configuration(
    log_ids=default_log_ids,
    concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
    resource_availability_type=ResourceAvailabilityType.SIMPLE,
    heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9)
)


def parallel_activities_with_heuristic_oracle(log: pd.DataFrame, log_ids: Optional[EventLogIDs] = None) -> Dict[
    str, set]:
    log_ids = log_ids_non_nil(log_ids)

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


def add_enabled_timestamp(log: pd.DataFrame):
    global default_configuration
    oracle = HeuristicsConcurrencyOracle(log, default_configuration)
    oracle.add_enabled_times(log)


def read_csv(log_path: Path, log_ids: Optional[EventLogIDs] = None, utc: bool = True) -> pd.DataFrame:
    log = pd.read_csv(log_path)

    log_ids = log_ids_non_nil(log_ids)

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


def log_ids_non_nil(log_ids: Optional[EventLogIDs]) -> EventLogIDs:
    """Returns non-nil event log columns."""

    if not log_ids:
        return default_log_ids

    return log_ids

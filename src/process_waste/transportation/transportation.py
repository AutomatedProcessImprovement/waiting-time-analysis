from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Callable, Dict

import click
import pandas as pd

from batch_processing_analysis.config import EventLogIDs
from . import handoff
from .. import WAITING_TIME_TOTAL_KEY, BATCH_INSTANCE_ENABLED_KEY, BATCH_INSTANCE_ID_KEY, \
    log_ids_non_nil, WAITING_TIME_BATCHING_KEY, WAITING_TIME_PRIORITIZATION_KEY, \
    WAITING_TIME_CONTENTION_KEY, WAITING_TIME_UNAVAILABILITY_KEY, WAITING_TIME_EXTRANEOUS_KEY, CTE_IMPACT_KEY
from ..core import core
from ..waiting_time import batching
from ..waiting_time.batching import BATCH_MIN_SIZE

REPORT_INDEX_COLUMNS = ['source_activity', 'source_resource', 'destination_activity', 'destination_resource']


def identify(
        log_path: Path,
        parallel_run=True,
        log_ids: Optional[EventLogIDs] = None,
        preprocessing_funcs: Optional[List[Callable]] = None,
        calendar: Optional[Dict] = None,
        batch_size: int = BATCH_MIN_SIZE) -> dict:
    log_ids = log_ids_non_nil(log_ids)

    log = core.read_csv(log_path, log_ids=log_ids)

    # preprocess event log
    if preprocessing_funcs is not None:
        for preprocess_func in preprocessing_funcs:
            click.echo(f'Preprocessing [{preprocess_func.__name__}]')
            log = preprocess_func(log)

    # NOTE: Batching analysis package adds enabled_timestamp column to the log
    # core.add_enabled_timestamp(log)

    # discarding unnecessary columns
    log = log[[log_ids.case, log_ids.activity, log_ids.resource, log_ids.start_time, log_ids.end_time]]

    # NB: sorting by end time is important for concurrency oracle that is run during batching analysis
    log.sort_values(by=[log_ids.end_time, log_ids.start_time, log_ids.activity], inplace=True)

    # taking batch creation time from the batch analysis
    log = batching.add_columns_from_batch_analysis(
        log,
        column_names=(BATCH_INSTANCE_ENABLED_KEY, BATCH_INSTANCE_ID_KEY),
        log_ids=log_ids,
        batch_size=batch_size)

    # total waiting time
    log[WAITING_TIME_TOTAL_KEY] = log[log_ids.start_time] - log[log_ids.enabled_time]

    # add_processing_time(log, log_ids)

    parallel_activities = core.parallel_activities_with_heuristic_oracle(log, log_ids=log_ids)
    handoff_report = handoff.identify(log, parallel_activities, parallel_run, log_ids=log_ids, calendar=calendar)

    process_cte_impact = calculate_cte_impact(handoff_report, log, log_ids=log_ids)

    return {'handoff': handoff_report, 'process_cte_impact': process_cte_impact}


@dataclass
class CTEImpactAnalysis:
    """Cycle time efficiency impact analysis."""
    batching_impact: float
    contention_impact: float
    prioritization_impact: float
    unavailability_impact: float
    extraneous_impact: float

    def to_json(self, filepath: Path):
        """Write CTE impact analysis to JSON file."""
        with filepath.open('w') as f:
            f.write(self.to_json_string())

    def to_json_string(self):
        """Return CTE impact analysis as JSON string."""
        return f'{{\n' \
               f'    "batching_impact": {self.batching_impact},\n' \
               f'    "contention_impact": {self.contention_impact},\n' \
               f'    "prioritization_impact": {self.prioritization_impact},\n' \
               f'    "unavailability_impact": {self.unavailability_impact},\n' \
               f'    "extraneous_impact": {self.extraneous_impact}\n' \
               f'}}'


def calculate_cte_impact(handoff_report, log: pd.DataFrame, log_ids: Optional[EventLogIDs] = None) -> CTEImpactAnalysis:
    """Calculates CTE impact of different types of wait time on the process level and transitions level."""
    log_ids = log_ids_non_nil(log_ids)

    # global CTE impact

    total_processing_time = log[log_ids.end_time].max() - log[log_ids.start_time].min()
    total_waiting_time = handoff_report[WAITING_TIME_TOTAL_KEY].sum()
    total_wt_batching = handoff_report[WAITING_TIME_BATCHING_KEY].sum()
    total_wt_prioritization = handoff_report[WAITING_TIME_PRIORITIZATION_KEY].sum()
    total_wt_contention = handoff_report[WAITING_TIME_CONTENTION_KEY].sum()
    total_wt_unavailability = handoff_report[WAITING_TIME_UNAVAILABILITY_KEY].sum()
    total_wt_extraneous = handoff_report[WAITING_TIME_EXTRANEOUS_KEY].sum()

    batching_impact = total_processing_time / (total_processing_time + total_waiting_time - total_wt_batching)
    contention_impact = total_processing_time / (total_processing_time + total_waiting_time - total_wt_contention)
    prioritization_impact = total_processing_time / (
            total_processing_time + total_waiting_time - total_wt_prioritization)
    unavailability_impact = total_processing_time / (
            total_processing_time + total_waiting_time - total_wt_unavailability)
    extraneous_impact = total_processing_time / (total_processing_time + total_waiting_time - total_wt_extraneous)

    result = CTEImpactAnalysis(
        batching_impact=batching_impact,
        contention_impact=contention_impact,
        prioritization_impact=prioritization_impact,
        unavailability_impact=unavailability_impact,
        extraneous_impact=extraneous_impact)

    # transitions CTE impact

    handoff_report[CTE_IMPACT_KEY] = total_processing_time / (
            total_processing_time + total_waiting_time - handoff_report[WAITING_TIME_TOTAL_KEY])

    return result


def __adjust_reports(handoff_report, pingpong_report):
    """Adjusts reports' waiting time, so that ping-pongs are not counted as hand-offs."""

    # identifying common records for both reports
    handoff_report = handoff_report.set_index(REPORT_INDEX_COLUMNS)

    if pingpong_report is not None and not pingpong_report.empty:
        pingpong_report = pingpong_report.set_index(REPORT_INDEX_COLUMNS)
        common_index = handoff_report.index.intersection(pingpong_report.index)

        # metrics to update
        wt_total_key = WAITING_TIME_TOTAL_KEY
        wt_total_sec_key = 'wt_total_seconds'
        frequency_key = 'frequency'

        _subtract_metrics_inplace(handoff_report, pingpong_report, common_index=common_index, metric_key=wt_total_key)
        _subtract_metrics_inplace(handoff_report, pingpong_report, common_index=common_index,
                                  metric_key=wt_total_sec_key)
        _subtract_metrics_inplace(handoff_report, pingpong_report, common_index=common_index, metric_key=frequency_key)

    # converting index back to columns
    handoff_report.reset_index(inplace=True)
    if pingpong_report is not None and not pingpong_report.empty:
        pingpong_report.reset_index(inplace=True)


def _subtract_metrics_inplace(df1: pd.DataFrame, df2: pd.DataFrame, common_index: pd.MultiIndex, metric_key: str):
    # if df1 have longer waiting time, subtract from df1
    df1_longer = df1.loc[common_index][metric_key] >= df2.loc[common_index][metric_key]
    df1_longer_index = df1.loc[common_index][df1_longer].index
    df1.loc[df1_longer_index, metric_key] = df1.loc[df1_longer_index][metric_key] - df2.loc[df1_longer_index][
        metric_key]

    # if df2 have longer waiting time, subtract from df2
    df2_longer = df1.loc[common_index][metric_key] < df2.loc[common_index][metric_key]
    df2_longer_index = df2.loc[common_index][df2_longer].index
    df2.loc[df2_longer_index, metric_key] = df2.loc[df2_longer_index][metric_key] - df1.loc[df2_longer_index][
        metric_key]

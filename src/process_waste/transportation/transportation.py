from pathlib import Path

import pandas as pd

from . import handoff
from . import pingpong
from .. import WAITING_TIME_TOTAL_KEY, START_TIMESTAMP_KEY, ENABLED_TIMESTAMP_KEY
from ..core import core
from ..waiting_time import batching, contention


def identify(log_path: Path, parallel_run=True) -> dict:
    log = core.read_csv(log_path)

    # NOTE: Batching analysis package adds enabled_timestamp column to the log
    # core.add_enabled_timestamp(log)

    # taking batch creation time from the batch analysis
    log = batching.add_columns_from_batch_analysis(log, column_names=('batch_creation_wt',))

    # total waiting time
    log[WAITING_TIME_TOTAL_KEY] = log[START_TIMESTAMP_KEY] - log[ENABLED_TIMESTAMP_KEY]

    log = contention.run_analysis(log)

    parallel_activities = core.parallel_activities_with_heuristic_oracle(log)

    handoff_report = handoff.identify(log, parallel_activities, parallel_run)
    pingpong_report = pingpong.identify(log, parallel_activities, parallel_run)

    # identifying common records for both reports
    index_columns = ['source_activity', 'source_resource', 'destination_activity', 'destination_resource']
    handoff_report = handoff_report.set_index(index_columns)

    if pingpong_report is not None and not pingpong_report.empty:
        pingpong_report = pingpong_report.set_index(index_columns)
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
    handoff_report = handoff_report.reset_index()
    if pingpong_report is not None and not pingpong_report.empty:
        pingpong_report = pingpong_report.reset_index()

    return {'handoff': handoff_report, 'pingpong': pingpong_report}


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

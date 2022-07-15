from pathlib import Path
from typing import Optional, List, Callable, Dict

import click

import process_waste.helpers
from batch_processing_analysis.config import EventLogIDs
from . import handoff
from .. import WAITING_TIME_TOTAL_KEY, BATCH_INSTANCE_ENABLED_KEY, log_ids_non_nil
from ..cte_impact import calculate_cte_impact
from ..helpers import BATCH_INSTANCE_ID_KEY
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

    log = process_waste.helpers.read_csv(log_path, log_ids=log_ids)

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

    parallel_activities = process_waste.helpers.parallel_activities_with_heuristic_oracle(log, log_ids=log_ids)
    handoff_report = handoff.identify(log, parallel_activities, parallel_run, log_ids=log_ids, calendar=calendar)

    process_cte_impact = calculate_cte_impact(handoff_report, log, log_ids=log_ids)

    return {'handoff': handoff_report, 'process_cte_impact': process_cte_impact}


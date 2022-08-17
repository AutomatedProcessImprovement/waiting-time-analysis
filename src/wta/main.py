from pathlib import Path
from typing import Optional, List, Callable, Dict

import click

from wta import log_ids_non_nil, activity_transitions, EventLogIDs, read_csv, \
    parallel_activities_with_heuristic_oracle
from wta.transitions_report import TransitionsReport
from wta.waiting_time import batching
from wta.waiting_time.batching import BATCH_MIN_SIZE

REPORT_INDEX_COLUMNS = ['source_activity', 'source_resource', 'destination_activity', 'destination_resource']


def run(log_path: Path,
        parallel_run=True,
        log_ids: Optional[EventLogIDs] = None,
        preprocessing_funcs: Optional[List[Callable]] = None,
        calendar: Optional[Dict] = None,
        batch_size: int = BATCH_MIN_SIZE) -> TransitionsReport:
    """
    Entry point for the project. It starts the main analysis which identifies activity transitions, and then uses them
    to analyze different types of waiting time.
    """

    log_ids = log_ids_non_nil(log_ids)

    log = read_csv(log_path, log_ids=log_ids)

    # preprocess event log
    if preprocessing_funcs is not None:
        for preprocess_func in preprocessing_funcs:
            click.echo(f'Preprocessing [{preprocess_func.__name__}]')
            log = preprocess_func(log)

    # discarding unnecessary columns
    log = log[[log_ids.case, log_ids.activity, log_ids.resource, log_ids.start_time, log_ids.end_time]]

    # NOTE: sorting by end time is important for concurrency oracle that is run during batching analysis
    log.sort_values(by=[log_ids.end_time, log_ids.start_time, log_ids.activity], inplace=True)

    # taking batch creation time from the batch analysis
    log = batching.add_columns_from_batch_analysis(
        log,
        column_names=(log_ids.batch_instance_enabled, log_ids.batch_id),
        log_ids=log_ids,
        batch_size=batch_size)
    # NOTE: Batching analysis package adds enabled_timestamp column to the log that is used later

    # total waiting time
    log[log_ids.wt_total] = log[log_ids.start_time] - log[log_ids.enabled_time]

    parallel_activities = parallel_activities_with_heuristic_oracle(log, log_ids=log_ids)
    transitions_data = activity_transitions.identify(log, parallel_activities, parallel_run, log_ids=log_ids,
                                                     calendar=calendar)

    transitions_report = TransitionsReport(transitions_data, log, log_ids)

    return transitions_report

from pathlib import Path
from typing import Optional, List, Callable, Dict, Union

import click
import pandas as pd

from batch_processing_discovery.discovery import discover_batches
from wta import log_ids_non_nil, activity_transitions, EventLogIDs, read_csv, \
    parallel_activities_with_heuristic_oracle, add_enabled_timestamp, compute_batch_activation_times, \
    print_section_boundaries
from wta.transitions_report import TransitionsReport

REPORT_INDEX_COLUMNS = ['source_activity', 'source_resource', 'destination_activity', 'destination_resource']


def run(log_path: Path,
        parallel_run=True,
        log_ids: Optional[EventLogIDs] = None,
        preprocessing_funcs: Optional[List[Callable]] = None,
        calendar: Optional[Dict] = None,
        log: Optional[pd.DataFrame] = None,
        group_results: bool = True) -> Union[TransitionsReport, Optional[pd.DataFrame]]:
    """
    Entry point for the project. It starts the main analysis which identifies activity transitions, and then uses them
    to analyze different types of waiting time.
    """
    log_ids = log_ids_non_nil(log_ids)

    if log is None:
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

    add_enabled_timestamp(log, log_ids)

    log = _batch_discovery(log, log_ids)

    # total waiting time
    log[log_ids.wt_total] = log[log_ids.start_time] - log[log_ids.enabled_time]

    parallel_activities = parallel_activities_with_heuristic_oracle(log, log_ids=log_ids)
    transitions_data = activity_transitions.identify(
        log, parallel_activities, parallel_run, log_ids=log_ids, calendar=calendar, group_results=group_results)

    if group_results:
        return TransitionsReport(transitions_data, log, log_ids)
    else:
        return transitions_data


@print_section_boundaries('Batch Analysis')
def _batch_discovery(log: pd.DataFrame, log_ids: EventLogIDs) -> pd.DataFrame:
    log = discover_batches(log, log_ids)
    return compute_batch_activation_times(log, log_ids)

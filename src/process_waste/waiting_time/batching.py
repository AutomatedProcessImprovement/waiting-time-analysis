import os
from typing import Optional

import click
import pandas as pd

from batch_processing_analysis.analysis import BatchProcessingAnalysis
from batch_processing_analysis.config import EventLogIDs, Configuration
from process_waste import print_section_boundaries, default_log_ids, BATCH_INSTANCE_ENABLED_KEY

RSCRIPT_BIN_PATH = os.environ.get('RSCRIPT_BIN_PATH')


def run_analysis(event_log: pd.DataFrame,
                 log_ids: EventLogIDs = default_log_ids,
                 rscript_path: str = '/usr/local/bin/Rscript') -> pd.DataFrame:
    global RSCRIPT_BIN_PATH

    config = Configuration()
    config.log_ids = log_ids
    config.PATH_R_EXECUTABLE = rscript_path if RSCRIPT_BIN_PATH is None else RSCRIPT_BIN_PATH
    config.report_batch_checkpoints = True
    config.min_batch_instance_size = 5
    click.echo(f'Running batch processing analysis with Rscript at: {config.PATH_R_EXECUTABLE}')
    try:
        return BatchProcessingAnalysis(event_log, config).analyze_batches()
    except Exception as e:
        click.echo(f'BatchProcessingAnalysis failed with exception: {e}')
        raise e


@print_section_boundaries('Batch analysis')
def add_columns_from_batch_analysis(
        log,
        column_names: tuple = (BATCH_INSTANCE_ENABLED_KEY,),
        log_ids: Optional[EventLogIDs] = None) -> pd.DataFrame:
    batch_log = run_analysis(log, log_ids=log_ids)
    log[log_ids.start_time] = log[log_ids.start_time].apply(__nullify_microseconds)
    log[log_ids.end_time] = log[log_ids.end_time].apply(__nullify_microseconds)
    result = pd.merge(
        log,
        batch_log[
            [log_ids.case, log_ids.activity, log_ids.start_time, log_ids.enabled_time, *column_names]
        ],
        how='left', on=[log_ids.case, log_ids.activity, log_ids.start_time])
    return result


def __nullify_microseconds(ts) -> pd.Timestamp:
    return pd.Timestamp(ts).replace(microsecond=0)

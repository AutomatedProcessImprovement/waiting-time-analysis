import os
from typing import Optional

import click
import pandas as pd

from batch_processing_analysis.analysis import BatchProcessingAnalysis
from batch_processing_analysis.config import Configuration
from wta.helpers import default_log_ids, print_section_boundaries, convert_timestamp_columns_to_datetime, \
    log_ids_non_nil, EventLogIDs

RSCRIPT_BIN_PATH = os.environ.get('RSCRIPT_BIN_PATH')
BATCH_MIN_SIZE = 1


def run(event_log: pd.DataFrame,
        log_ids: EventLogIDs = None,
        rscript_path: str = '/usr/local/bin/Rscript',
        batch_size: int = BATCH_MIN_SIZE) -> pd.DataFrame:
    global RSCRIPT_BIN_PATH

    config = Configuration()
    config.log_ids = log_ids if log_ids else default_log_ids
    config.PATH_R_EXECUTABLE = rscript_path if RSCRIPT_BIN_PATH is None else RSCRIPT_BIN_PATH
    config.report_batch_checkpoints = True
    config.min_batch_instance_size = batch_size
    click.echo(f'Running batch processing analysis with Rscript at: {config.PATH_R_EXECUTABLE}')
    try:
        return BatchProcessingAnalysis(event_log, config).analyze_batches()
    except Exception as e:
        click.echo(f'BatchProcessingAnalysis failed with exception: {e}')
        raise e


@print_section_boundaries('Batch Analysis')
def add_columns_from_batch_analysis(
        log,
        column_names: tuple = (EventLogIDs().batch_instance_enabled,),
        log_ids: Optional[EventLogIDs] = None,
        batch_size: int = BATCH_MIN_SIZE) -> pd.DataFrame:
    log_ids = log_ids_non_nil(log_ids)

    batch_log = run(log, log_ids=log_ids, batch_size=batch_size)
    log[log_ids.start_time] = log[log_ids.start_time].apply(__nullify_microseconds)
    log[log_ids.end_time] = log[log_ids.end_time].apply(__nullify_microseconds)
    result = pd.merge(
        log,
        batch_log[
            [log_ids.case, log_ids.activity, log_ids.start_time, log_ids.enabled_time, *column_names]
        ],
        how='left', on=[log_ids.case, log_ids.activity, log_ids.start_time])

    result = convert_timestamp_columns_to_datetime(result, log_ids, (log_ids.batch_instance_enabled,))

    return result


def __nullify_microseconds(ts) -> pd.Timestamp:
    return pd.Timestamp(ts).replace(microsecond=0)

import click
import pandas as pd

from batch_processing_analysis.analysis import BatchProcessingAnalysis
from batch_processing_analysis.config import EventLogIDs, Configuration
from process_waste import CASE_KEY, ACTIVITY_KEY, START_TIMESTAMP_KEY, ENABLED_TIMESTAMP_KEY, print_section_boundaries
from process_waste.core import core


def default_log_ids() -> EventLogIDs:
    log_ids = EventLogIDs()
    log_ids.start_time = core.START_TIMESTAMP_KEY
    log_ids.end_time = core.END_TIMESTAMP_KEY
    log_ids.enabled_time = core.ENABLED_TIMESTAMP_KEY
    log_ids.resource = core.RESOURCE_KEY
    log_ids.case = core.CASE_KEY
    log_ids.activity = core.ACTIVITY_KEY
    return log_ids


def run_analysis(event_log: pd.DataFrame,
                 log_ids: EventLogIDs = default_log_ids(),
                 rscript_path: str = '/usr/local/bin/Rscript') -> pd.DataFrame:
    config = Configuration()
    config.log_ids = log_ids
    config.PATH_R_EXECUTABLE = rscript_path
    config.report_batch_checkpoints = True
    try:
        return BatchProcessingAnalysis(event_log, config).analyze_batches()
    except Exception as e:
        click.echo(f'BatchProcessingAnalysis failed with exception: {e}')
        raise e


@print_section_boundaries('Batch analysis')
def add_columns_from_batch_analysis(log, column_names: tuple = ('batch_creation_wt',)) -> pd.DataFrame:
    batch_log = run_analysis(log)
    result = pd.merge(log,
                      batch_log[
                          [CASE_KEY, ACTIVITY_KEY, START_TIMESTAMP_KEY, ENABLED_TIMESTAMP_KEY, *column_names]],
                      how='left', on=[CASE_KEY, ACTIVITY_KEY, START_TIMESTAMP_KEY])
    return result

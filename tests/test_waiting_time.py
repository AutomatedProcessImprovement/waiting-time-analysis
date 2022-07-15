from pathlib import Path

import pandas as pd
import pytest

import process_waste.helpers
from process_waste import WAITING_TIME_TOTAL_KEY
from process_waste.helpers import START_TIMESTAMP_KEY, ENABLED_TIMESTAMP_KEY
from process_waste.waiting_time import batching


def read_event_log(log_path: Path) -> pd.DataFrame:
    log = process_waste.helpers.read_csv(log_path)
    process_waste.helpers.add_enabled_timestamp(log)
    log[WAITING_TIME_TOTAL_KEY] = log[START_TIMESTAMP_KEY] - log[ENABLED_TIMESTAMP_KEY]
    return log


@pytest.mark.integration
def test_batch_processing_analysis(assets_path):
    preprocessed_log_path = assets_path / 'PurchasingExample.csv'

    # Read and preprocess event log
    event_log = pd.read_csv(preprocessed_log_path)
    event_log[process_waste.helpers.START_TIMESTAMP_KEY] = pd.to_datetime(event_log[
                                                                              process_waste.helpers.START_TIMESTAMP_KEY], utc=True)
    event_log[process_waste.helpers.END_TIMESTAMP_KEY] = pd.to_datetime(event_log[
                                                                            process_waste.helpers.END_TIMESTAMP_KEY], utc=True)

    # Run main analysis
    batch_event_log = batching.run_analysis(event_log)

    assert batch_event_log is not None
    assert 'batch_creation_wt' in batch_event_log.columns
    assert 'batch_ready_wt' in batch_event_log.columns
    assert 'batch_other_wt' in batch_event_log.columns
    assert not batch_event_log['batch_creation_wt'].isna().all()


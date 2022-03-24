import pandas as pd

from process_waste.core import core
from process_waste.waiting_time import batching


def test_batch_processing_analysis_pkg(assets_path):
    preprocessed_log_path = assets_path / 'PurchasingExample.csv'

    # Read and preprocess event log
    event_log = pd.read_csv(preprocessed_log_path)
    event_log[core.START_TIMESTAMP_KEY] = pd.to_datetime(event_log[core.START_TIMESTAMP_KEY], utc=True)
    event_log[core.END_TIMESTAMP_KEY] = pd.to_datetime(event_log[core.END_TIMESTAMP_KEY], utc=True)

    # Run main analysis
    batch_event_log = batching.run_analysis(event_log)

    assert batch_event_log is not None
    assert 'batch_creation_wt' in batch_event_log.columns
    assert 'batch_ready_wt' in batch_event_log.columns
    assert 'batch_other_wt' in batch_event_log.columns
    assert not batch_event_log['batch_creation_wt'].isna().all()

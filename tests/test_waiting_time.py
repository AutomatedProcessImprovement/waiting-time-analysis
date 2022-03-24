import pandas as pd
import pytest

from process_waste import WAITING_TIME_CONTENTION_KEY
from process_waste.core import core
from process_waste.waiting_time import batching, contention


def test_batch_processing_analysis(assets_path):
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


@pytest.mark.log_path('resource_contention.csv')
def test_resource_contention_for_event(event_log):
    event_index = pd.Index([0])
    result = contention.contention_for_event(event_index, event_log)

    assert result is not None
    assert WAITING_TIME_CONTENTION_KEY in result.columns
    assert result[WAITING_TIME_CONTENTION_KEY].sum() > pd.Timedelta(0)
    assert result.loc[event_index, WAITING_TIME_CONTENTION_KEY].sum() == pd.Timedelta(hours=2, minutes=30)


@pytest.mark.log_path('resource_contention.csv')
def test_resource_contention_analysis(event_log):
    result = contention.run_analysis(event_log)

    assert result is not None
    assert WAITING_TIME_CONTENTION_KEY in result.columns
    assert result[WAITING_TIME_CONTENTION_KEY].sum() > pd.Timedelta(0)

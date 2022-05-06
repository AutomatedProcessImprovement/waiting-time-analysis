from pathlib import Path

import pandas as pd
import pytest

from process_waste import WAITING_TIME_CONTENTION_KEY, WAITING_TIME_PRIORITIZATION_KEY, WAITING_TIME_TOTAL_KEY, \
    START_TIMESTAMP_KEY, ENABLED_TIMESTAMP_KEY
from process_waste.core import core
from process_waste.waiting_time import batching
from process_waste.waiting_time.prioritization_and_contention import detect_prioritization_or_contention, \
    run_analysis as prioritization_and_contention_analysis


def read_event_log(log_path: Path) -> pd.DataFrame:
    log = core.read_csv(log_path)
    core.add_enabled_timestamp(log)
    log[WAITING_TIME_TOTAL_KEY] = log[START_TIMESTAMP_KEY] - log[ENABLED_TIMESTAMP_KEY]
    return log


@pytest.mark.integration
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


@pytest.mark.parametrize('log_name, event_index, expected_contention', [
    ('resource_contention.csv', 1, pd.Timedelta(hours=2, minutes=30)),
])
def test_detect_contention(assets_path: Path, log_name: str, event_index: int, expected_contention: pd.Timedelta):
    log_path = assets_path / log_name
    event_log = core.read_csv(log_path)
    pd_event_index = pd.Index([event_index])
    detect_prioritization_or_contention(pd_event_index, event_log)
    assert event_log.loc[pd_event_index, WAITING_TIME_CONTENTION_KEY].sum() == expected_contention


def test_detect_prioritization_or_contention(assets_path):
    log_path = assets_path / 'prioritization_and_contention.csv'
    event_log = core.read_csv(log_path)

    event_index = pd.Index([2])
    detect_prioritization_or_contention(event_index, event_log)

    assert event_log is not None
    assert WAITING_TIME_PRIORITIZATION_KEY in event_log.columns
    assert WAITING_TIME_CONTENTION_KEY in event_log.columns
    assert event_log[WAITING_TIME_PRIORITIZATION_KEY].sum() > pd.Timedelta(0)
    assert event_log[WAITING_TIME_CONTENTION_KEY].sum() > pd.Timedelta(0)


def test_prioritization_and_contention_analysis(assets_path):
    log_path = assets_path / 'prioritization_and_contention.csv'
    event_log = core.read_csv(log_path)

    result = prioritization_and_contention_analysis(event_log)

    assert result is not None
    assert WAITING_TIME_PRIORITIZATION_KEY in result.columns
    assert WAITING_TIME_CONTENTION_KEY in result.columns
    assert result[WAITING_TIME_PRIORITIZATION_KEY].sum() > pd.Timedelta(0)
    assert result[WAITING_TIME_CONTENTION_KEY].sum() > pd.Timedelta(0)
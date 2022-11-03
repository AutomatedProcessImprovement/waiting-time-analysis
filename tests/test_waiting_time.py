from pathlib import Path

import pandas as pd
import pytest

import wta.helpers
from wta import WAITING_TIME_TOTAL_KEY
from wta.helpers import START_TIMESTAMP_KEY, ENABLED_TIMESTAMP_KEY
from wta.waiting_time import batching
from wta.waiting_time.analysis import __subtract_intervals_a_from_intervals_b_non_recursive, \
    __subtract_a_from_intervals_b


def read_event_log(log_path: Path) -> pd.DataFrame:
    log = wta.helpers.read_csv(log_path)
    wta.helpers.add_enabled_timestamp(log)
    log[WAITING_TIME_TOTAL_KEY] = log[START_TIMESTAMP_KEY] - log[ENABLED_TIMESTAMP_KEY]
    return log


@pytest.mark.integration
def test_batch_processing_analysis(assets_path):
    preprocessed_log_path = assets_path / 'PurchasingExample.csv'

    # Read and preprocess event log
    event_log = pd.read_csv(preprocessed_log_path)
    event_log[wta.helpers.START_TIMESTAMP_KEY] = pd.to_datetime(event_log[
                                                                    wta.helpers.START_TIMESTAMP_KEY], utc=True)
    event_log[wta.helpers.END_TIMESTAMP_KEY] = pd.to_datetime(event_log[
                                                                  wta.helpers.END_TIMESTAMP_KEY], utc=True)

    # Run main analysis
    batch_event_log = batching.run(event_log)

    assert batch_event_log is not None
    assert 'batch_creation_wt' in batch_event_log.columns
    assert 'batch_ready_wt' in batch_event_log.columns
    assert 'batch_other_wt' in batch_event_log.columns
    assert not batch_event_log['batch_creation_wt'].isna().all()


interval_test_data = [
    {
        'name': 'A',
        'a': [pd.Interval(0, 5)],
        'b': [pd.Interval(0, 10)],
        'expected': [pd.Interval(5, 10)]
    },
    {
        'name': 'B',
        'a': [pd.Interval(0, 5), pd.Interval(6, 10)],
        'b': [pd.Interval(0, 10)],
        'expected': [pd.Interval(5, 6)]
    },
    {
        'name': 'C',
        'a': [pd.Interval(0, 5), pd.Interval(6, 10)],
        'b': [pd.Interval(0, 5), pd.Interval(6, 10)],
        'expected': []
    },
    {
        'name': 'D',
        'a': [pd.Interval(5, 7), pd.Interval(7, 10)],
        'b': [pd.Interval(0, 5), pd.Interval(10, 20)],
        'expected': [pd.Interval(0, 5), pd.Interval(10, 20)]
    },
    {
        'name': 'E',
        'a': [pd.Interval(5, 10)],
        'b': [pd.Interval(0, 5), pd.Interval(10, 20)],
        'expected': [pd.Interval(0, 5), pd.Interval(10, 20)]
    },
    {
        'name': 'F',
        'a': [pd.Interval(5, 10)],
        'b': [],
        'expected': []
    },
    {
        'name': 'G',
        'a': [pd.Interval(5, 5)],
        'b': [pd.Interval(5, 10)],
        'expected': [pd.Interval(5, 10)]
    },
    {
        'name': 'H',
        'a': [pd.Interval(10, 10)],
        'b': [pd.Interval(5, 10)],
        'expected': [pd.Interval(5, 10)]
    },
    {
        'name': 'I',
        'a': [],
        'b': [],
        'expected': []
    },
]


@pytest.mark.parametrize('test_case', interval_test_data, ids=[test_data['name'] for test_data in interval_test_data])
def test_analysis_subtract_a_from_intervals_b(test_case):
    a = test_case['a']
    b = test_case['b']
    result = __subtract_intervals_a_from_intervals_b_non_recursive(a, b)
    assert result == test_case['expected']


@pytest.mark.parametrize('test_case', interval_test_data, ids=[test_data['name'] for test_data in interval_test_data])
def test_analysis_subtract_intervals_a_from_intervals_b_non_recursive(test_case):
    a = test_case['a']
    b = test_case['b']
    result = __subtract_intervals_a_from_intervals_b_non_recursive(a, b)
    assert result == test_case['expected']


singular_from_intervals_test_data = [
    {
        'name': 'A',
        'a': pd.Interval(0, 5),
        'b': [pd.Interval(0, 10)],
        'expected': [pd.Interval(5, 10)]
    },
    {
        'name': 'B',
        'a': pd.Interval(0, 10),
        'b': [pd.Interval(0, 10)],
        'expected': []
    },
    {
        'name': 'C',
        'a': pd.Interval(20, 30),
        'b': [pd.Interval(0, 10)],
        'expected': [pd.Interval(0, 10)]
    },
    {
        'name': 'D',
        'a': pd.Interval(0, 0),
        'b': [pd.Interval(0, 10)],
        'expected': [pd.Interval(0, 10)]
    },
    {
        'name': 'E',
        'a': pd.Interval(2, 3),
        'b': [pd.Interval(0, 10)],
        'expected': [pd.Interval(0, 2), pd.Interval(3, 10)]
    },
]


@pytest.mark.parametrize('test_case', singular_from_intervals_test_data,
                         ids=[test_data['name'] for test_data in singular_from_intervals_test_data])
def test_analysis_subtract_a_from_intervals_b(test_case):
    a = test_case['a']
    b = test_case['b']
    result = __subtract_a_from_intervals_b(a, b)
    assert result == test_case['expected']

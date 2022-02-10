from typing import List

import pandas as pd
import pytest
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.log.util import interval_lifecycle
from waste import core
from waste.core import timezone_aware_subtraction


@pytest.fixture
def case(assets_path) -> pd.DataFrame:
    case = pd.read_csv(assets_path / 'bimp-example_case_409.csv')
    case['start_timestamp'] = pd.to_datetime(case['start_timestamp'])
    case['time:timestamp'] = pd.to_datetime(case['time:timestamp'])
    return case.sort_values(by='time:timestamp')


@pytest.fixture
def case_enabled(assets_path) -> pd.DataFrame:
    case = pd.read_csv(assets_path / 'bimp-example_case_409_enabled.csv')
    case['start_timestamp'] = pd.to_datetime(case['start_timestamp'])
    case['time:timestamp'] = pd.to_datetime(case['time:timestamp'])
    return case.sort_values(by='time:timestamp')


def test_get_interval_log(bimp_example_path):
    log = core.lifecycle_to_interval(bimp_example_path)
    assert log is not None


def test_get_concurrent_activities(cases):
    for case in cases:
        activities = core.get_concurrent_activities(case)
        assert activities is not None
        assert len(activities) == 1
        assert len(activities[0]) == 2


def test_alpha_oracle(bimp_example_path):
    log_interval_df = core.lifecycle_to_interval(bimp_example_path)
    result = core.parallel_activities_with_alpha_oracle(log_interval_df)
    assert result is not None


def test_concurrent_activities_by_time(bimp_example_path):
    log_interval_df = core.lifecycle_to_interval(bimp_example_path)
    result = core.concurrent_activities_by_time(log_interval_df)
    assert result is not None


def test_timezone_aware_subtraction():
    df1 = pd.DataFrame({'timestamp': [pd.Timestamp('2017-02-01 13:00+0200')]})
    df2 = pd.DataFrame({'timestamp': [pd.Timestamp('2017-02-01 14:00+0300')]})
    assert (timezone_aware_subtraction(df1, df2, 'timestamp') == pd.Series([pd.Timedelta(0)])).all()


@pytest.fixture
def handoffs(assets_path) -> List[pd.DataFrame]:
    return [
        pd.read_csv(assets_path / 'bimp-example_case_handoff_1.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_handoff_2.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_handoff_3.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_handoff_4.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_handoff_5.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_handoff_6.csv'),
    ]

def test_join_handoffs(handoffs):
    result = core.join_per_case_items(handoffs)
    assert result is not None and not result.empty

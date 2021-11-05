from pathlib import Path
from typing import List

import pandas as pd
import pytest
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.log.util import interval_lifecycle

from waste import core


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


def test_calculate_enabled_timestamps(case_enabled):
    case = core.add_enabled_timestamps(case_enabled)
    assert case is not None
    assert 'enabled_timestamp' in case.keys()


def test_enabled_timestamps_case(bimp_example_path, case):
    log = xes_importer.apply(str(bimp_example_path))
    event_log = log_converter.apply(log, variant=log_converter.Variants.TO_DATA_FRAME)

    case_409_assign = event_log.query('`lifecycle:transition` == "assign" & `case:concept:name` == "409"')
    case_409_assign = case_409_assign.rename(columns={'time:timestamp': 'assign_timestamp'})
    truth = pd.merge(case, case_409_assign[['elementId', 'assign_timestamp']], on='elementId', how='left')
    test_results = core.add_enabled_timestamps(case)
    assert (test_results['enabled_timestamp'] == truth['assign_timestamp']).all()


def test_enabled_timestamps_all(bimp_example_path):
    log = xes_importer.apply(str(bimp_example_path))
    log_df = log_converter.apply(log, variant=log_converter.Variants.TO_DATA_FRAME)
    log_interval = interval_lifecycle.to_interval(log)
    log_interval_df = log_converter.apply(log_interval, variant=log_converter.Variants.TO_DATA_FRAME)

    grouped = log_interval_df.groupby(by='case:concept:name')
    concurrent_activities = core.parallel_activities_with_alpha_oracle(log_interval_df)
    for case_id, case in grouped:
        case_with_assign = log_df.query('`lifecycle:transition` == "assign" & `case:concept:name` == @case_id')
        case_with_assign = case_with_assign.rename(columns={'time:timestamp': 'assign_timestamp'})
        case_with_enabled = core.add_enabled_timestamps(case, concurrent_activities)
        case_with_assign = case_with_assign.sort_values(by='assign_timestamp')
        case_with_enabled = case_with_enabled.sort_values(by='enabled_timestamp')

        comparison = case_with_assign.assign_timestamp.values == case_with_enabled.enabled_timestamp.values
        assert comparison.all(), f'In {bimp_example_path} for case {case_id} timestamps do not match'


def test_alpha_oracle(bimp_example_path):
    log_interval_df = core.lifecycle_to_interval(bimp_example_path)
    result = core.parallel_activities_with_alpha_oracle(log_interval_df)
    assert result is not None


def test_concurrent_activities_by_time(bimp_example_path):
    log_interval_df = core.lifecycle_to_interval(bimp_example_path)
    result = core.concurrent_activities_by_time(log_interval_df)
    assert result is not None

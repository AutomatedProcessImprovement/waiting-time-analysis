from pathlib import Path
from typing import List

import pandas as pd
import pytest

from waste import core


@pytest.fixture
def bimp_example_path(assets_path) -> Path:
    return assets_path / 'BIMP_example.xes'


@pytest.fixture
def case(assets_path) -> pd.DataFrame:
    case = pd.read_csv(assets_path / 'bimp-example_case_409.csv')
    case['start_timestamp'] = pd.to_datetime(case['start_timestamp'])
    case['time:timestamp'] = pd.to_datetime(case['time:timestamp'])
    return case.sort_values(by='start_timestamp')


@pytest.fixture
def cases(assets_path) -> List[pd.DataFrame]:
    cases = [
        pd.read_csv(assets_path / 'bimp-example_case_21.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_272.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_293.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_409.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_444.csv'),
    ]

    def _preprocess(case):
        case['start_timestamp'] = pd.to_datetime(case['start_timestamp'])
        case['time:timestamp'] = pd.to_datetime(case['time:timestamp'])
        return case.sort_values(by='start_timestamp')

    return list(map(_preprocess, cases))


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


def test_get_interval_log(bimp_example_path):
    log = core.lifecycle_to_interval(bimp_example_path)
    assert log is not None


def test_get_concurrent_activities(cases):
    for case in cases:
        activities = core.get_concurrent_activities(case)
        assert activities is not None
        assert len(activities) == 1
        assert len(activities[0]) == 2


def test_make_aliases_for_concurrent_activities(cases):
    for case in cases:
        activities = core.get_concurrent_activities(case)
        aliases = core.make_aliases_for_concurrent_activities(case, activities)
        assert aliases is not None


def test_replace_concurrent_activities_with_aliases(cases):
    for case in cases:
        activities = core.get_concurrent_activities(case)
        aliases = core.make_aliases_for_concurrent_activities(case, activities)
        case_with_aliases = core.replace_concurrent_activities_with_aliases(case, activities, aliases)
        assert case_with_aliases is not None


def test_identify_sequential_handoffs(bimp_example_path):
    log = core.lifecycle_to_interval(bimp_example_path)
    log_grouped = log.groupby(by='case:concept:name')
    case = log_grouped.get_group('409').sort_values(by='start_timestamp')
    activities = core.get_concurrent_activities(case)
    aliases = core.make_aliases_for_concurrent_activities(case, activities)
    case_with_aliases = core.replace_concurrent_activities_with_aliases(case, activities, aliases)
    handoffs = core.identify_sequential_handoffs(case_with_aliases)
    assert handoffs is not None


def test_identify_concurrent_handoffs(cases):
    for case in cases:
        activities = core.get_concurrent_activities(case)
        aliases = core.make_aliases_for_concurrent_activities(case, activities)
        case_with_aliases = core.replace_concurrent_activities_with_aliases(case, activities, aliases)
        handoffs = core.identify_concurrent_handoffs(case_with_aliases, aliases)
        assert handoffs is not None


def test_identify_handoffs(cases):
    for case in cases:
        handoffs = core.identify_handoffs(case)
        assert handoffs is not None


def test_identify_handoffs_all_cases(bimp_example_path):
    log = core.lifecycle_to_interval(bimp_example_path)
    log_grouped = log.groupby(by='case:concept:name')
    all_handoffs = []
    for (case_id, case) in log_grouped:
        case = case.sort_values(by='start_timestamp')
        handoffs = core.identify_handoffs(case)
        if handoffs is not None:
            all_handoffs.append(handoffs)
    assert len(all_handoffs) > 0


def test_join_handoffs(bimp_example_path, handoffs):
    result = core.join_handoffs(handoffs)
    assert result is not None and not result.empty

from typing import List

import pandas as pd
import pytest

from waste import core, handoff


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


def test_make_aliases_for_concurrent_activities(cases):
    for case in cases:
        activities = core.get_concurrent_activities(case)
        aliases = handoff.make_aliases_for_concurrent_activities(case, activities)
        assert aliases is not None


def test_replace_concurrent_activities_with_aliases(cases):
    for case in cases:
        activities = core.get_concurrent_activities(case)
        aliases = handoff.make_aliases_for_concurrent_activities(case, activities)
        case_with_aliases = handoff.replace_concurrent_activities_with_aliases(case, activities, aliases)
        assert case_with_aliases is not None


def test_identify_sequential_handoffs(bimp_example_path):
    log = core.lifecycle_to_interval(bimp_example_path)
    log_grouped = log.groupby(by='case:concept:name')
    case = log_grouped.get_group('409').sort_values(by='start_timestamp')
    case = core.add_enabled_timestamps(case)
    activities = core.get_concurrent_activities(case)
    aliases = handoff.make_aliases_for_concurrent_activities(case, activities)
    case_with_aliases = handoff.replace_concurrent_activities_with_aliases(case, activities, aliases)
    handoffs = handoff.identify_sequential_handoffs(case_with_aliases)
    assert handoffs is not None


def test_identify_concurrent_handoffs(cases):
    for case in cases:
        case = core.add_enabled_timestamps(case)
        activities = core.get_concurrent_activities(case)
        aliases = handoff.make_aliases_for_concurrent_activities(case, activities)
        case_with_aliases = handoff.replace_concurrent_activities_with_aliases(case, activities, aliases)
        handoffs = handoff.identify_concurrent_handoffs(case_with_aliases, aliases)
        assert handoffs is not None


def test_identify_handoffs(cases):
    for case in cases:
        handoffs = handoff.identify_handoffs(case)
        assert handoffs is not None


def test_identify_handoffs_all_cases(bimp_example_path):
    log = core.lifecycle_to_interval(bimp_example_path)
    log_grouped = log.groupby(by='case:concept:name')
    all_handoffs = []
    for (case_id, case) in log_grouped:
        case = case.sort_values(by='start_timestamp')
        handoffs = handoff.identify_handoffs(case)
        if handoffs is not None:
            all_handoffs.append(handoffs)
    assert len(all_handoffs) > 0


def test_join_handoffs(handoffs):
    result = handoff.join_handoffs(handoffs)
    assert result is not None and not result.empty

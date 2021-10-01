from pathlib import Path

import pytest

from waste import core


@pytest.fixture
def bimp_example_path(assets_path) -> Path:
    return assets_path / 'BIMP_example.xes'


def test_get_interval_log(bimp_example_path):
    log = core.lifecycle_to_interval(bimp_example_path)
    assert log is not None


def test_get_concurrent_activities(bimp_example_path):
    log = core.lifecycle_to_interval(bimp_example_path)
    log_grouped = log.groupby(by='case:concept:name')
    case = log_grouped.get_group('409').sort_values(by='start_timestamp')
    activities = core.get_concurrent_activities(case)
    assert activities is not None
    assert len(activities) == 1
    assert len(activities[0]) == 2


def test_make_aliases_for_concurrent_activities(bimp_example_path):
    log = core.lifecycle_to_interval(bimp_example_path)
    log_grouped = log.groupby(by='case:concept:name')
    case = log_grouped.get_group('409').sort_values(by='start_timestamp')
    activities = core.get_concurrent_activities(case)
    aliases = core.make_aliases_for_concurrent_activities(case, activities)
    assert aliases is not None


def test_replace_concurrent_activities_with_aliases(bimp_example_path):
    log = core.lifecycle_to_interval(bimp_example_path)
    log_grouped = log.groupby(by='case:concept:name')
    case = log_grouped.get_group('409').sort_values(by='start_timestamp')
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


def test_identify_concurrent_handoffs(bimp_example_path):
    log = core.lifecycle_to_interval(bimp_example_path)
    log_grouped = log.groupby(by='case:concept:name')
    case = log_grouped.get_group('409').sort_values(by='start_timestamp')
    activities = core.get_concurrent_activities(case)
    aliases = core.make_aliases_for_concurrent_activities(case, activities)
    case_with_aliases = core.replace_concurrent_activities_with_aliases(case, activities, aliases)
    handoffs = core.identify_concurrent_handoffs(case_with_aliases, aliases)
    assert handoffs is not None


def test_identify_handoffs(bimp_example_path):
    log = core.lifecycle_to_interval(bimp_example_path)
    log_grouped = log.groupby(by='case:concept:name')
    case = log_grouped.get_group('409').sort_values(by='start_timestamp')
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


def test_join_handoffs(bimp_example_path):
    log = core.lifecycle_to_interval(bimp_example_path)
    log_grouped = log.groupby(by='case:concept:name')
    all_handoffs = []
    for (case_id, case) in log_grouped:
        case = case.sort_values(by='start_timestamp')
        handoffs = core.identify_handoffs(case)
        if handoffs is not None:
            all_handoffs.append(handoffs)
    result = core.join_handoffs(all_handoffs)
    assert result is not None and not result.empty

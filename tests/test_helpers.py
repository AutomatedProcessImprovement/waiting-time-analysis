import pytest

from wta import EventLogIDs

column_mapping_cases = ["""
{
    "case": "case:concept:name",
    "activity": "concept:name",
    "start_timestamp": "start_timestamp",
    "end_timestamp": "time:timestamp",
    "resource": "org:resource"
}
"""]

column_mapping_cases_dicts = [
    {
        "case": "case:concept:name",
        "activity": "concept:name",
        "start_timestamp": "start_timestamp",
        "end_timestamp": "time:timestamp",
        "resource": "org:resource"
    }
]


@pytest.mark.parametrize('test_data', column_mapping_cases)
def test_EventLogIDs_from_json(test_data):
    log_ids = EventLogIDs.from_json(test_data)
    assert log_ids.case == 'case:concept:name'
    assert log_ids.activity == 'concept:name'
    assert log_ids.resource == 'org:resource'
    assert log_ids.start_time == 'start_timestamp'
    assert log_ids.end_time == 'time:timestamp'


@pytest.mark.parametrize('test_data', column_mapping_cases_dicts)
def test_EventLogIDs_from_dict(test_data):
    log_ids = EventLogIDs.from_dict(test_data)
    assert log_ids.case == 'case:concept:name'
    assert log_ids.activity == 'concept:name'
    assert log_ids.resource == 'org:resource'
    assert log_ids.start_time == 'start_timestamp'
    assert log_ids.end_time == 'time:timestamp'

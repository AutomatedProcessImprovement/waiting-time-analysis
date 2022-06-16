import pandas as pd
import pytest

from process_waste import add_enabled_timestamp
from process_waste import handoff
from process_waste.calendar import calendar
from process_waste.core import core
from process_waste.core import read_csv
from process_waste.transportation.handoff import __identify_handoffs_per_case_and_make_report, __mark_strict_handoffs, \
    __remove_overlapping_time_from_intervals


@pytest.mark.integration
@pytest.mark.log_path('PurchasingExample.csv')
def test_negative_duration(event_log, config):
    parallel_activities = core.parallel_activities_with_heuristic_oracle(event_log)
    result = handoff.identify(event_log, parallel_activities, parallel_run=False)
    assert sum(result['wt_total_seconds'] < 0) == 0


def test_strict_handoffs_occurred(assets_path):
    log_path = assets_path / 'PurchasingExampleCase1.csv'
    log = read_csv(log_path)
    add_enabled_timestamp(log)
    result = __mark_strict_handoffs(log)
    assert result is not None
    assert 'handoff_type' in result.columns
    assert 'strict' in result['handoff_type'].values


# @pytest.mark.log_path('PurchasingExampleCase1.csv')
# def test_identify_self_handoff(event_log):
#     parallel_activities = {}
#     case_id = '1'
#     log_calendar = calendar.make(event_log, granularity=15)
#     result = __identify_handoffs_per_case_and_make_report(
#         event_log,
#         parallel_activities=parallel_activities,
#         case_id=case_id,
#         log=event_log,
#         log_calendar=log_calendar)
#     assert 'handoff_type' in result.columns
#     assert 'self' in result['handoff_type'].values
#     assert 'strict' in result['handoff_type'].values
#
#     assert result[(result['source_activity'] == 'Create Purchase Requisition')
#                   & (result['source_resource'] == 'Kim Passa')
#                   & (result['destination_activity'] == 'Create Request for Quotation')
#                   & (result['destination_resource'] == 'Kim Passa')
#                   ]['handoff_type'].values[0] == 'self'
#
#     assert result[(result['source_activity'] == 'Create Request for Quotation')
#                   & (result['source_resource'] == 'Kim Passa')
#                   & (result['destination_activity'] == 'Analyze Request for Quotation')
#                   & (result['destination_resource'] == 'Karel de Groot')
#                   ]['handoff_type'].values[0] == 'strict'


# Pandas intervals

overlapping_intervals_test_cases = [
    {
        'test_case_name': 'overlapping intervals',
        'input': [
            pd.Interval(pd.Timestamp('2020-01-01 00:00:00'), pd.Timestamp('2020-01-01 01:00:00')),
            pd.Interval(pd.Timestamp('2020-01-01 00:30:00'), pd.Timestamp('2020-01-01 01:30:00')),
            pd.Interval(pd.Timestamp('2020-01-01 01:00:00'), pd.Timestamp('2020-01-01 02:00:00')),
        ],
        'output': [
            pd.Interval(pd.Timestamp('2020-01-01 00:00:00'), pd.Timestamp('2020-01-01 01:00:00')),
            pd.Interval(pd.Timestamp('2020-01-01 01:00:00'), pd.Timestamp('2020-01-01 01:30:00')),
            pd.Interval(pd.Timestamp('2020-01-01 01:30:00'), pd.Timestamp('2020-01-01 02:00:00')),
        ]
    },
    {
        'test_case_name': 'non-overlapping intervals',
        'input': [
            pd.Interval(pd.Timestamp('2020-01-01 00:00:00'), pd.Timestamp('2020-01-01 01:00:00')),
            pd.Interval(pd.Timestamp('2020-01-01 01:00:00'), pd.Timestamp('2020-01-01 02:00:00')),
            pd.Interval(pd.Timestamp('2020-01-01 02:30:00'), pd.Timestamp('2020-01-01 03:00:00')),
        ],
        'output': [
            pd.Interval(pd.Timestamp('2020-01-01 00:00:00'), pd.Timestamp('2020-01-01 01:00:00')),
            pd.Interval(pd.Timestamp('2020-01-01 01:00:00'), pd.Timestamp('2020-01-01 02:00:00')),
            pd.Interval(pd.Timestamp('2020-01-01 02:30:00'), pd.Timestamp('2020-01-01 03:00:00')),
        ]
    },
    {
        'test_case_name': 'overlapping intervals across days [A]',
        'input': [
            pd.Interval(pd.Timestamp('2020-01-01 19:00:00'), pd.Timestamp('2020-01-02 05:00:00')),
            pd.Interval(pd.Timestamp('2020-01-01 23:00:00'), pd.Timestamp('2020-01-02 04:00:00')),
        ],
        'output': [
            pd.Interval(pd.Timestamp('2020-01-01 19:00:00'), pd.Timestamp('2020-01-02 05:00:00')),
        ]
    },
    {
        'test_case_name': 'overlapping intervals across days [B]',
        'input': [
            pd.Interval(pd.Timestamp('2020-01-01 19:00:00'), pd.Timestamp('2020-01-02 02:00:00')),
            pd.Interval(pd.Timestamp('2020-01-01 23:00:00'), pd.Timestamp('2020-01-02 04:00:00')),
        ],
        'output': [
            pd.Interval(pd.Timestamp('2020-01-01 19:00:00'), pd.Timestamp('2020-01-02 02:00:00')),
            pd.Interval(pd.Timestamp('2020-01-02 02:00:00'), pd.Timestamp('2020-01-02 04:00:00')),
        ]
    },
    {
        'test_case_name': 'non-overlapping intervals across days',
        'input': [
            pd.Interval(pd.Timestamp('2020-01-01 00:00:00'), pd.Timestamp('2020-01-01 08:00:00')),
            pd.Interval(pd.Timestamp('2020-01-01 23:00:00'), pd.Timestamp('2020-01-02 01:00:00')),
        ],
        'output': [
            pd.Interval(pd.Timestamp('2020-01-01 00:00:00'), pd.Timestamp('2020-01-01 08:00:00')),
            pd.Interval(pd.Timestamp('2020-01-01 23:00:00'), pd.Timestamp('2020-01-02 01:00:00')),
        ]
    },
    {
        'test_case_name': 'empty list',
        'input': [],
        'output': []
    },
    {
        'test_case_name': 'single item',
        'input': [
            pd.Interval(pd.Timestamp('2020-01-01 00:00:00'), pd.Timestamp('2020-01-01 08:00:00')),
        ],
        'output': [
            pd.Interval(pd.Timestamp('2020-01-01 00:00:00'), pd.Timestamp('2020-01-01 08:00:00')),
        ]
    },
]


@pytest.mark.parametrize('test_data', overlapping_intervals_test_cases,
                         ids=map(lambda x: x['test_case_name'], overlapping_intervals_test_cases))
def test__remove_overlapping_time_from_intervals(test_data):
    result = __remove_overlapping_time_from_intervals(test_data['input'])
    assert result == test_data['output']

import pandas as pd
import pytest

from wta.waiting_time.analysis import __remove_overlapping_time_from_intervals

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

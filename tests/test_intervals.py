from typing import List

import pandas as pd
import pytest

from process_waste.calendar.intervals import Interval, WeekDay, subtract_intervals, pd_interval_to_interval, \
    remove_overlapping_time_from_intervals


class TestIntervals:
    def test_subtract_intervals_a(self):
        intervals_1 = [
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='05:37:00',
                     right_time='07:23:00'),
        ]
        intervals_2 = [
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='05:37:00',
                     right_time='05:45:00'),
        ]
        expected_result = [
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='05:45:00',
                     right_time='07:23:00'),
        ]
        result = subtract_intervals(intervals_1, intervals_2)
        assert result == expected_result

    def test_subtract_intervals_b(self):
        intervals_1 = [
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='05:37:00',
                     right_time='07:23:00'),
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='08:00:00',
                     right_time='08:30:00'),
        ]
        intervals_2 = [
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='05:37:00',
                     right_time='05:45:00'),
        ]
        expected_result = [
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='05:45:00',
                     right_time='07:23:00'),
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='08:00:00',
                     right_time='08:30:00'),
        ]
        result = subtract_intervals(intervals_1, intervals_2)
        assert result == expected_result

    def test_subtract_intervals_c(self):
        intervals_1 = [
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='05:37:00',
                     right_time='07:23:00'),
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='08:00:00',
                     right_time='08:30:00'),
        ]
        intervals_2 = [
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='05:37:00',
                     right_time='05:45:00'),
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='08:10:00',
                     right_time='08:20:00'),
        ]
        expected_result = [
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='05:45:00',
                     right_time='07:23:00'),
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='08:00:00',
                     right_time='08:10:00'),
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='08:20:00',
                     right_time='08:30:00'),
        ]
        result = subtract_intervals(intervals_1, intervals_2)
        assert result == expected_result

    def test_subtract_intervals_d(self):
        intervals_1 = [
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='05:37:00',
                     right_time='07:23:00'),
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='08:03:00',
                     right_time='10:37:00'),
        ]
        intervals_2 = [
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='05:37:00',
                     right_time='05:45:00'),
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='06:45:00',
                     right_time='07:00:00'),
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='07:15:00',
                     right_time='07:30:00'),
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='08:00:00',
                     right_time='08:30:00'),
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='10:30:00',
                     right_time='10:37:00'),
        ]
        expected_result = [
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='05:45:00',
                     right_time='06:45:00'),
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='07:00:00',
                     right_time='07:15:00'),
            Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY, left_time='08:30:00',
                     right_time='10:30:00'),
        ]
        result = subtract_intervals(intervals_1, intervals_2)
        assert result == expected_result

    @pytest.mark.parametrize('interval,expected', [
        (
                pd.Interval(pd.Timestamp('2022-04-28 08:00:00'), pd.Timestamp('2022-04-28 10:00:00')),
                [Interval(left_day=WeekDay.THURSDAY, right_day=WeekDay.THURSDAY,
                          left_time='08:00:00.000000', right_time='10:00:00.000000')]
        ),
        (
                pd.Interval(pd.Timestamp('2022-04-28 08:00:00'), pd.Timestamp('2022-04-29 01:00:00')),
                [Interval(left_day=WeekDay.THURSDAY, right_day=WeekDay.THURSDAY,
                          left_time='08:00:00.000000', right_time='23:59:59.999999'),
                 Interval(left_day=WeekDay.FRIDAY, right_day=WeekDay.FRIDAY,
                          left_time='00:00:00.000000', right_time='01:00:00.000000')]
        ),
        (
                pd.Interval(pd.Timestamp('2022-04-28 08:00:00'), pd.Timestamp('2022-04-30 01:00:00')),
                [Interval(left_day=WeekDay.THURSDAY, right_day=WeekDay.THURSDAY,
                          left_time='08:00:00.000000', right_time='23:59:59.999999'),
                 Interval(left_day=WeekDay.FRIDAY, right_day=WeekDay.FRIDAY,
                          left_time='00:00:00.000000', right_time='23:59:59.999999'),
                 Interval(left_day=WeekDay.SATURDAY, right_day=WeekDay.SATURDAY,
                          left_time='00:00:00.000000', right_time='01:00:00.000000')]
        )
    ])
    def test_pd_interval_to_interval(self, interval: pd.Interval, expected: List[Interval]):
        result = pd_interval_to_interval(interval)
        assert result == expected


overlapping_intervals_test_cases = [
    {
        'test_case_name': 'overlapping intervals',
        'input': [
            Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='00:00:00', right_time='01:00:00'),
            Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='00:30:00', right_time='01:30:00'),
            Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='01:00:00', right_time='02:00:00'),
        ],
        'output': [
            Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='00:00:00', right_time='01:00:00'),
            Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='01:00:00', right_time='01:30:00'),
            Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='01:30:00', right_time='02:00:00'),
        ]
    },
    {
        'test_case_name': 'non-overlapping intervals',
        'input': [
            Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='00:00:00', right_time='01:00:00'),
            Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='01:00:00', right_time='02:00:00'),
            Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='02:30:00', right_time='03:00:00'),
        ],
        'output': [
            Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='00:00:00', right_time='01:00:00'),
            Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='01:00:00', right_time='02:00:00'),
            Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='02:30:00', right_time='03:00:00'),
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
            Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='00:00:00', right_time='01:00:00'),
        ],
        'output': [
            Interval(left_day=WeekDay.MONDAY, right_day=WeekDay.MONDAY, left_time='00:00:00', right_time='01:00:00'),
        ]
    },
]


@pytest.mark.parametrize('test_data', overlapping_intervals_test_cases,
                         ids=map(lambda x: x['test_case_name'], overlapping_intervals_test_cases))
def test__remove_overlapping_time_from_intervals(test_data):
    result = remove_overlapping_time_from_intervals(test_data['input'])
    assert result == test_data['output']

from pathlib import Path
from typing import List

import pandas as pd
import pytest

from waste import handoff, core


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
    result = handoff._join_per_case_handoffs(handoffs)
    assert result is not None and not result.empty


def test_negative_duration(assets_path):
    log_path = assets_path / 'PurchasingExample.xes'
    result = handoff.identify(log_path)
    assert sum(result['duration_sum_seconds'] < 0) == 0


def test_ping_pong_identification(assets_path):
    args = [
        {
            'name': 'A',
            'log_path': assets_path / 'PurchasingExample.xes',
            'case_path': assets_path / 'ping_pong_case.csv',
            'expected': [{'source_activity': 'Create Request for Quotation',
                          'source_resource': 'Kim Passa',
                          'destination_activity': 'Analyze Request for Quotation',
                          'destination_resource': 'Karel de Groot'}]
        },
        {
            'name': 'B',
            'log_path': assets_path / 'PurchasingExample.xes',
            'case_path': assets_path / 'ping_pong_case_fake.csv',
            'expected': []
        },
        {
            'name': 'C',
            'log_path': assets_path / 'PurchasingExample.xes',
            'case_path': assets_path / 'double_ping_pong_case.csv',
            'expected': [{'source_activity': 'Create Request for Quotation',
                          'source_resource': 'Kim Passa',
                          'destination_activity': 'Analyze Request for Quotation',
                          'destination_resource': 'Karel de Groot'},
                         {'source_activity': 'Approve Purchase Order for payment',
                          'source_resource': 'Karel de Groot',
                          'destination_activity': 'Send Invoice',
                          'destination_resource': 'Kiu Kan'}]
        }
    ]

    for arg in args:
        print(f"\rTest case {arg['name']}")
        log_path: Path = arg['log_path']
        case_path: Path = arg['case_path']
        expected = arg['expected']

        log = core.lifecycle_to_interval(log_path)
        parallel_activities = core.parallel_activities_with_heuristic_oracle(log)

        case = pd.read_csv(case_path)
        assert case is not None and len(case) > 0

        result = handoff._identify_ping_pongs_per_case(case, parallel_activities)
        assert result == expected

from pathlib import Path

import pandas as pd

from waste import core, pingpong


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

        result = pingpong._identify_ping_pongs_per_case(case, parallel_activities)
        assert result == expected

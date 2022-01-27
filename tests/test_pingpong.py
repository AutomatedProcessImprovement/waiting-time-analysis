from pathlib import Path

import pandas as pd
from waste import core, pingpong


def test_ping_pong_identification(assets_path):
    args = [
        {
            'name': 'A',
            'log_path': assets_path / 'PurchasingExample.xes',
            'case_path': assets_path / 'ping_pong_case.csv',
            'expected': pd.DataFrame([
                {'source_activity': 'Create Request for Quotation',
                 'source_resource': 'Kim Passa',
                 'destination_activity': 'Analyze Request for Quotation',
                 'destination_resource': 'Karel de Groot',
                 'frequency': 1,
                 'duration': pd.to_timedelta(60.0, 'seconds'),
                 'case_id': '1'}]
            )
        },
        {
            'name': 'B',
            'log_path': assets_path / 'PurchasingExample.xes',
            'case_path': assets_path / 'ping_pong_case_fake.csv',
            'expected': pd.DataFrame(columns=['case_id'])
        },
        {
            'name': 'C',
            'log_path': assets_path / 'PurchasingExample.xes',
            'case_path': assets_path / 'two_ping_pongs_case.csv',
            'expected': pd.DataFrame([
                {'source_activity': 'Create Request for Quotation',
                 'source_resource': 'Kim Passa',
                 'destination_activity': 'Analyze Request for Quotation',
                 'destination_resource': 'Karel de Groot',
                 'frequency': 1,
                 'duration': pd.to_timedelta(60.0, 'seconds'),
                 'case_id': '1'},
                {'source_activity': 'Approve Purchase Order for payment',
                 'source_resource': 'Karel de Groot',
                 'destination_activity': 'Send Invoice',
                 'destination_resource': 'Kiu Kan',
                 'frequency': 1,
                 'duration': pd.to_timedelta(0, 'seconds'),
                 'case_id': '1'}])
        },
        {
            'name': 'D',
            'log_path': assets_path / 'PurchasingExample.xes',
            'case_path': assets_path / 'double_ping_pong_case.csv',
            'expected': pd.DataFrame([
                {'source_activity': 'Create Request for Quotation',
                 'source_resource': 'Kim Passa',
                 'destination_activity': 'Analyze Request for Quotation',
                 'destination_resource': 'Karel de Groot',
                 'frequency': 2,
                 'duration': pd.to_timedelta(3780.0, 'seconds'),
                 'case_id': '1'},
                {'source_activity': 'Analyze Request for Quotation',
                 'source_resource': 'Karel de Groot',
                 'destination_activity': 'Create Request for Quotation',
                 'destination_resource': 'Kim Passa',
                 'frequency': 1,
                 'duration': pd.to_timedelta(3720.0, 'seconds'),
                 'case_id': '1'}])
        },
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

        result = pingpong._identify_ping_pongs_per_case(case, parallel_activities, case_id='1')
        assert ((result.reset_index() == expected.reset_index()).all()).all()


def test_ping_pong_identify(assets_path):
    log_path = assets_path / 'PurchasingExample.xes'
    result = pingpong.identify(log_path, parallel_run=False)
    assert sum(result['duration_sum_seconds'] < 0) == 0

from pathlib import Path

import pandas as pd
import pytest

from process_waste import core, pingpong, WAITING_TIME_TOTAL_KEY


@pytest.mark.integration
def test_ping_pong_identification(assets_path):
    args = [
        {
            'name': 'A',
            'log_path': assets_path / 'PurchasingExample.csv',
            'case_path': assets_path / 'ping_pong_case.csv',
            'expected': pd.DataFrame([
                {'source_activity': 'Create Request for Quotation',
                 'source_resource': 'Kim Passa',
                 'destination_activity': 'Analyze Request for Quotation',
                 'destination_resource': 'Karel de Groot',
                 'frequency': 1,
                 WAITING_TIME_TOTAL_KEY: pd.to_timedelta(2 * 60 * 60 + 11 * 60, 'seconds'),
                 'case_id': '1'}]
            )
        },
        {
            'name': 'B',
            'log_path': assets_path / 'PurchasingExample.csv',
            'case_path': assets_path / 'ping_pong_case_fake.csv',
            'expected': pd.DataFrame(columns=['case_id'])
        },
        {
            'name': 'C',
            'log_path': assets_path / 'PurchasingExample.csv',
            'case_path': assets_path / 'two_ping_pongs_case.csv',
            'expected': pd.DataFrame([
                {'source_activity': 'Create Request for Quotation',
                 'source_resource': 'Kim Passa',
                 'destination_activity': 'Analyze Request for Quotation',
                 'destination_resource': 'Karel de Groot',
                 'frequency': 1,
                 WAITING_TIME_TOTAL_KEY: pd.to_timedelta(2 * 60 * 60 + 11 * 60, 'seconds'),
                 'case_id': '1'},
                {'source_activity': 'Approve Purchase Order for payment',
                 'source_resource': 'Karel de Groot',
                 'destination_activity': 'Send Invoice',
                 'destination_resource': 'Kiu Kan',
                 'frequency': 1,
                 WAITING_TIME_TOTAL_KEY: pd.to_timedelta(5 * 60 * 60 + 45 * 60, 'seconds'),
                 'case_id': '1'}])
        },
        {
            'name': 'D',
            'log_path': assets_path / 'PurchasingExample.csv',
            'case_path': assets_path / 'double_ping_pong_case.csv',
            'expected': pd.DataFrame([
                {'source_activity': 'Create Request for Quotation',
                 'source_resource': 'Kim Passa',
                 'destination_activity': 'Analyze Request for Quotation',
                 'destination_resource': 'Karel de Groot',
                 'frequency': 2,
                 WAITING_TIME_TOTAL_KEY: pd.to_timedelta(5 * 60 * 60 + 28 * 60, 'seconds'),
                 'case_id': '1'},
                {'source_activity': 'Analyze Request for Quotation',
                 'source_resource': 'Karel de Groot',
                 'destination_activity': 'Create Request for Quotation',
                 'destination_resource': 'Kim Passa',
                 'frequency': 1,
                 WAITING_TIME_TOTAL_KEY: pd.to_timedelta(3 * 60 * 60 + 3 * 60, 'seconds'),
                 'case_id': '1'}])
        },
    ]

    for arg in args:
        print(f"\rTest case {arg['name']}")
        log_path: Path = arg['log_path']
        case_path: Path = arg['case_path']
        expected = arg['expected']

        log = core.read_csv(log_path)
        core.add_enabled_timestamp(log)
        parallel_activities = core.parallel_activities_with_heuristic_oracle(log)

        case = core.read_csv(case_path)
        core.add_enabled_timestamp(case)
        assert case is not None and len(case) > 0

        result = pingpong._identify_ping_pongs_per_case(
            case,
            parallel_activities=parallel_activities,
            case_id='1',
            enabled_on=True)
        assert ((result.reset_index() == expected.reset_index()).all()).all()


@pytest.mark.integration
@pytest.mark.log_path('PurchasingExample.csv')
def test_ping_pong_identify(event_log):
    parallel_activities = core.parallel_activities_with_heuristic_oracle(event_log)
    core.add_enabled_timestamp(event_log)
    result = pingpong.identify(event_log, parallel_activities, parallel_run=False)
    assert sum(result['wt_total_seconds'] < 0) == 0

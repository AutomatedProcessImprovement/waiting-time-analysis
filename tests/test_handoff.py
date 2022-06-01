import pytest

from process_waste import add_enabled_timestamp
from process_waste import handoff
from process_waste.calendar import calendar
from process_waste.core import core
from process_waste.core import read_csv
from process_waste.transportation.handoff import __identify_handoffs_per_case_and_make_report, __mark_strict_handoffs


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


@pytest.mark.log_path('PurchasingExampleCase1.csv')
def test_identify_self_handoff(event_log):
    parallel_activities = {}
    case_id = '1'
    log_calendar = calendar.make(event_log, granularity=15)
    result = __identify_handoffs_per_case_and_make_report(
        event_log,
        parallel_activities=parallel_activities,
        case_id=case_id,
        log=event_log,
        log_calendar=log_calendar)
    assert 'handoff_type' in result.columns
    assert 'self' in result['handoff_type'].values
    assert 'strict' in result['handoff_type'].values

    assert result[(result['source_activity'] == 'Create Purchase Requisition')
                  & (result['source_resource'] == 'Kim Passa')
                  & (result['destination_activity'] == 'Create Request for Quotation')
                  & (result['destination_resource'] == 'Kim Passa')
                  ]['handoff_type'].values[0] == 'self'

    assert result[(result['source_activity'] == 'Create Request for Quotation')
                  & (result['source_resource'] == 'Kim Passa')
                  & (result['destination_activity'] == 'Analyze Request for Quotation')
                  & (result['destination_resource'] == 'Karel de Groot')
                  ]['handoff_type'].values[0] == 'strict'

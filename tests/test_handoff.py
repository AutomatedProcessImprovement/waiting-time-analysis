import pytest
from estimate_start_times.concurrency_oracle import HeuristicsConcurrencyOracle

from process_waste import handoff
from process_waste.core import core


@pytest.mark.log_path('PurchasingExample.csv')
def test_negative_duration(event_log, config):
    oracle = HeuristicsConcurrencyOracle(event_log, config)
    oracle.add_enabled_times(event_log)

    parallel_activities = core.parallel_activities_with_heuristic_oracle(event_log)
    result = handoff.identify(event_log, parallel_activities, parallel_run=False)
    assert sum(result['duration_sum_seconds'] < 0) == 0

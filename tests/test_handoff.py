import pytest
from estimate_start_times.concurrency_oracle import HeuristicsConcurrencyOracle

from process_waste import handoff
from process_waste.core import core


@pytest.mark.log_path('PurchasingExample.csv')
def test_negative_duration(event_log, config):
    log = event_log
    config = config

    oracle = HeuristicsConcurrencyOracle(log, config)
    oracle.add_enabled_times(log)

    parallel_activities = core.parallel_activities_with_heuristic_oracle(log)
    result = handoff.identify(log, parallel_activities, parallel_run=False)
    assert sum(result['duration_sum_seconds'] < 0) == 0

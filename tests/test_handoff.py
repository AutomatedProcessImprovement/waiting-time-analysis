from waste import handoff
from waste.core import core


def test_negative_duration(assets_path):
    log_path = assets_path / 'PurchasingExample.xes'
    log = core.lifecycle_to_interval(log_path)
    parallel_activities = core.parallel_activities_with_heuristic_oracle(log)
    result = handoff.identify(log, parallel_activities, parallel_run=False)
    assert sum(result['duration_sum_seconds'] < 0) == 0

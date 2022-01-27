from waste import handoff


def test_negative_duration(assets_path):
    log_path = assets_path / 'PurchasingExample.xes'
    result = handoff.identify(log_path)
    assert sum(result['duration_sum_seconds'] < 0) == 0

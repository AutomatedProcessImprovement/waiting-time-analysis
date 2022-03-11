from estimate_start_times.concurrency_oracle import HeuristicsConcurrencyOracle


def test_enabled_timestamp_calculation_purchasing_example(event_log_parametrized, config):
    log = event_log_parametrized
    config = config

    oracle = HeuristicsConcurrencyOracle(log, config)
    oracle.add_enabled_times(log)
    assert log is not None
    assert all(log['enabled_timestamp'].isna()) is False
    assert all(log['enabled_timestamp'].isnull()) is False

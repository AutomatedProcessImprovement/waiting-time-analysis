import pandas as pd
from estimate_start_times.concurrency_oracle import HeuristicsConcurrencyOracle
from estimate_start_times.config import Configuration, DEFAULT_XES_IDS
from waste.core import core


def test_enabled_timestamp_calculation(assets_path):
    log_path = assets_path / 'Production.xes'
    log = core.lifecycle_to_interval(log_path)
    log['start_timestamp'] = pd.to_datetime(log['start_timestamp'], utc=True)
    log['time:timestamp'] = pd.to_datetime(log['time:timestamp'], utc=True)
    config = Configuration(log_ids=DEFAULT_XES_IDS)
    config.log_ids.start_time = 'start_timestamp'
    config.log_ids.enabled_time = 'enabled_timestamp'
    config.log_ids.available_time = 'available_timestamp'
    config.log_ids.case = 'case:concept:name'
    oracle = HeuristicsConcurrencyOracle(log, config)
    oracle.add_enabled_times(log)
    assert log is not None

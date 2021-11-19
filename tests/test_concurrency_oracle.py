from config import Configuration, DEFAULT_XES_IDS, ConcurrencyOracleType, ResourceAvailabilityType, \
    HeuristicsThresholds, ReEstimationMethod
from event_log.concurrency_oracle import HeuristicsConcurrencyOracle as EventLogHeuristics
from event_log_readers import read_event_log


def test_pm4py_heuristics(assets_path):
    log_path = assets_path / 'Production.xes'

    config = Configuration(
        log_ids=DEFAULT_XES_IDS,
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        bot_resources={"Start", "End"},
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9)
    )

    log = read_event_log(str(log_path), config)
    oracle = EventLogHeuristics(log, config)

    assert oracle is not None

from config import Configuration, DEFAULT_XES_IDS, ConcurrencyOracleType, ResourceAvailabilityType, \
    HeuristicsThresholds, ReEstimationMethod, EventLogIDs
from data_frame.concurrency_oracle import HeuristicsConcurrencyOracle as DFHeuristics
from event_log.concurrency_oracle import HeuristicsConcurrencyOracle as ELHeuristics
from event_log_readers import read_event_log
from waste import core


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
    oracle = ELHeuristics(log, config)

    assert oracle is not None


def test_data_frame_heuristics(assets_path):
    log_path = assets_path / 'Production.xes'
    log = core.lifecycle_to_interval(log_path)

    column_names = EventLogIDs(
        case='case:concept:name',
        activity='concept:name',
        start_timestamp='start_timestamp',
        end_timestamp='time:timestamp',
        resource='org:resource',
        lifecycle='lifecycle:transition'
    )

    config = Configuration(
        log_ids=column_names,
        re_estimation_method=ReEstimationMethod.MODE,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        bot_resources={"Start", "End"},
        heuristics_thresholds=HeuristicsThresholds(df=0.9, l2l=0.9)
    )

    oracle = DFHeuristics(log, config)

    assert oracle is not None

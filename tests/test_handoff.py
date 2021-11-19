from typing import List

import pandas as pd
import pytest

from waste import handoff


@pytest.fixture
def handoffs(assets_path) -> List[pd.DataFrame]:
    return [
        pd.read_csv(assets_path / 'bimp-example_case_handoff_1.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_handoff_2.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_handoff_3.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_handoff_4.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_handoff_5.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_handoff_6.csv'),
    ]


def test_join_handoffs(handoffs):
    result = handoff._join_per_case_handoffs(handoffs)
    assert result is not None and not result.empty


def test_negative_duration(assets_path):
    log_path = assets_path / 'PurchasingExample.xes'
    result = handoff.identify(log_path)
    assert sum(result['duration_sum_seconds'] < 0) == 0
